#!/usr/bin/env python3
# vim: fileencoding=utf-8

from os import makedirs, getenv, sep as os_sep, walk as os_walk
from re import compile as re_compile
from bz2 import open as bz2_open
from csv import writer as csv_writer
from time import localtime, strftime
from hashlib import md5, sha1
from logging import getLogger, StreamHandler, FileHandler, Formatter, INFO, DEBUG
from os.path import isfile, isdir, dirname, basename, expanduser, join as path_join
from sqlite3 import connect as sqlite_connect
from platform import system
from collections import defaultdict, namedtuple, deque
from configparser import ConfigParser


def getint(data, offset, intsize):
  """ Retrieve an integer (big-endian) and new offset from the current offset """
  value = 0
  while intsize > 0:
    value = (value << 8) + data[offset]
    offset += 1
    intsize -= 1
  return value, offset

def getbytes(data, offset):
  """ Retrieve a string and new offset from the current offset into the data """
  if data[offset] == 0xFF and data[offset + 1] == 0xFF:
    return b'', offset + 2  # Blank string
  length, offset = getint(data, offset, 2)  # 2-byte length
  value = data[offset:offset + length]
  return value, (offset + length)

def getstr(data, offset):
  value, offset = getbytes(data, offset)
  value = value.decode('utf8')
  return value, offset

def process_mbdb_file(filename):
  mbdb = {}
  data = open(filename, "rb").read()
  if data[0:4] != b"mbdb":
    raise Exception("This does not look like an MBDB file")
  offset = 4
  offset += 2  # value x05 x00, not sure what this is
  size = len(data)
  while offset < size:
    fileinfo = {}
    fileinfo['start_offset'] = offset
    fileinfo['domain'],     offset = getstr(data, offset)
    fileinfo['filename'],   offset = getstr(data, offset)
    fileinfo['linktarget'], offset = getstr(data, offset)
    fileinfo['datahash'],   offset = getbytes(data, offset)
    fileinfo['unknown1'],   offset = getstr(data, offset)
    fileinfo['mode'],       offset = getint(data, offset, 2)
    fileinfo['unknown2'],   offset = getint(data, offset, 4)
    fileinfo['unknown3'],   offset = getint(data, offset, 4)
    fileinfo['userid'],     offset = getint(data, offset, 4)
    fileinfo['groupid'],    offset = getint(data, offset, 4)
    fileinfo['mtime'],      offset = getint(data, offset, 4)
    fileinfo['atime'],      offset = getint(data, offset, 4)
    fileinfo['ctime'],      offset = getint(data, offset, 4)
    fileinfo['filelen'],    offset = getint(data, offset, 8)
    fileinfo['flag'],       offset = getint(data, offset, 1)
    fileinfo['numprops'],   offset = getint(data, offset, 1)
    fileinfo['properties'] = {}
    for i in range(fileinfo['numprops']):
      propname, offset = getstr(data, offset)
      propval,  offset = getbytes(data, offset)
      fileinfo['properties'][propname] = propval
    fullpath = '%s-%s' % (fileinfo['domain'], fileinfo['filename'])
    fileinfo['filehash'] = sha1(fullpath.encode('utf8')).hexdigest()
    mbdb[fileinfo['start_offset']] = fileinfo
  return mbdb


class Sqlite(object):
  def __init__(self, fname):
    if not isfile(fname):
      raise Exception('Database does not exist: %s' % fname)
    self.fname = fname

  def __enter__(self):
    self.con = sqlite_connect(self.fname)
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.con.close()

  def get_query(self, query):
    cur = self.con.cursor()
    cur.execute(query)
    return cur.fetchall()


class Wechat(object):
  _conf_file = path_join(dirname(__file__), 'conf-wechat-exporter.ini')
  def __init__(self):
    self._fn_pat = re_compile(r'[<>;:"/|?*\\]+')
    self._init_logger()

  def _init_logger(self):
    self.L = getLogger('L')
    self.L.handlers.clear()
    self.L.setLevel(DEBUG)
    handler = StreamHandler()
    handler.setFormatter(Formatter('%(message)s'))
    self.L.addHandler(handler)

  def _load_manifest_db(self, db):
    # Manifest.mbdb
    if db.endswith('.mbdb'):
      return process_mbdb_file(db)
    # Manifest.db
    mbdb = {}
    with Sqlite(db) as con:
      tables = con.get_query('SELECT fileID, relativePath FROM Files WHERE domain="AppDomain-com.tencent.xin" AND relativePath!=""')
      for fid, path in tables:
        fileinfo = {}
        fileinfo['filename'] = path
        fileinfo['filehash'] = '%s%s%s' % (fid[:2], os_sep, fid)
        mbdb[fid] = fileinfo
    return mbdb

  def _load_contacts(self, db):
    with Sqlite(db) as con:
      tables = con.get_query('SELECT userName, dbContactRemark, dbContactProfile, dbContactChatRoom FROM Friend')
      contacts = {}
      groups_id = {}
      filenames = defaultdict(set)
      for user, remark, profile, room in tables:
        namehash = md5(user.encode('utf8')).hexdigest()
        mmid, nickname, dispname = self._parse_name(remark)
        filename = self._get_valid_filename((dispname, nickname, mmid, user))
        filenames[filename].add(namehash)
        country, state, city, signature = self._parse_profile(profile)
        contacts[namehash] = (user, mmid, nickname, dispname, country, state, city, signature)
        if room:
          group_name = self._get_valid_filename((dispname, nickname, mmid, user))
          groups_id[group_name] = self._get_group_info(room)
      groups = {k: [self._get_contact_info(x, contacts) for x in v] for k, v in groups_id.items()}
      duplicates = set()
      for f, hashes in filenames.items():
        if len(hashes) > 1:
          duplicates |= hashes
      return contacts, groups, duplicates

  def _load_chats(self, db):
    with Sqlite(db) as con:
      tables = con.get_query('SELECT name FROM sqlite_master WHERE type="table";')
      for table in tables:
        if table[0].startswith('Chat_'):
          namehash = table[0][5:]
          yield (namehash,
              con.get_query('SELECT CreateTime, Type, Des, Message FROM %s;' % table[0]))

  def _parse_name(self, remark):
    # remark[0] is '\n'
    length = remark[1]
    offset = 2
    nickname = remark[2:offset + length].decode('utf8')
    mmid, dispname = '', ''
    offset += length
    while True:
      if len(remark) <= offset:
        break
      if remark[offset] == 0x1a:  # custom dispname
        offset += 1
        length = remark[offset]
        offset += 1
        dispname = remark[offset:offset + length].decode('utf8')
        if mmid and dispname:
          break
        offset += length
      elif remark[offset] == 0x12:  # wechat name
        offset += 1
        length = remark[offset]
        offset += 1
        mmid = remark[offset:offset + length].decode('utf8')
        if mmid and dispname:
          break
        offset += length
      elif remark[offset] in (0x22, 0x2a, 0x32, 0x3a):
        offset += 1
        length = remark[offset]
        offset += 1
        offset += length
      elif remark[offset] == 0x42:
        offset += 1
        length = remark[offset]
        offset += 1
        if length != 0 and remark[offset] == 0x01:
          offset += 1
        offset += length
      else:
        offset += 1
        length = remark[offset]
        offset += 1
        offset += length
    return mmid, nickname, dispname

  def _parse_profile(self, profile):
    # profile[0] is '\b'
    offset = 1
    country, state, city, signature = [''] * 4
    if profile[offset] > 0:
      offset += 1
      while True:
        if len(profile) <= offset:
          break
        if profile[offset] == 0x12:  # Country
          offset += 1
          length = profile[offset]
          offset += 1
          country = profile[offset:offset + length].decode('utf8')
          offset += length
        elif profile[offset] == 0x1a:  # State
          offset += 1
          length = profile[offset]
          offset += 1
          state = profile[offset:offset + length].decode('utf8')
          offset += length
        elif profile[offset] == 0x22:  # City
          offset += 1
          length = profile[offset]
          offset += 1
          city = profile[offset:offset + length].decode('utf8')
          offset += length
        elif profile[offset] == 0x2a:  # Signature
          offset += 1
          length = profile[offset]
          offset += 1
          signature = profile[offset:offset + length].decode('utf8')
          offset += length
    return (country, state, city, signature)

  def _get_group_info(self, room):
    if not room:
      return []
    # room[0] is '\n'
    length = room[1]
    if length & 0b10000000:
      length = (room[1] & 0b01111111) + (room[2] << 7)
      offset = 3
    else:
      offset = 2
    members = room[offset:offset + length].decode('utf8')
    return members.split(';')

  def _get_msg_type(self, tp, content):
    if tp == 50:
      if content == 'voip_content_voice':
        return '语音通话'
      elif content == 'voip_content_video':
        return '视频通话'
    return {
      1: '文本',
      3: '图片',
      34: '语音',
      35: '邮件',
      42: '名片',
      43: '视频',
      44: '视频',
      47: '表情',
      48: '位置',
      49: '链接',
      50: '通话',
      62: '视频',
      64: '语音通话',
      10000: '系统消息',
      10002: '撤回的消息'
    }[tp]

  def _get_msg_direction(self, des):
    return {
      1: '接收',
      0: '发送',
    }[des]

  def _get_contact_info(self, mmid, contacts):
    if not mmid:
      return [''] * (len(contacts[next(iter(contacts))]))
    namehash = md5(mmid.encode('utf8')).hexdigest()
    try:
      return contacts[namehash]
    except KeyError:
      return [mmid] + [''] * (len(contacts[next(iter(contacts))]) - 1)

  def _get_sender(self, msg, contacts):
    sender = msg.split(':\n', 1)
    if len(sender) == 2:
      info = self._get_contact_info(sender[0], contacts)
      return info, sender[1]
    return self._get_contact_info(None, contacts), msg

  def _get_valid_filename(self, names):
    for name in names:
      if name:
        n = self._fn_pat.sub('', name).encode(
            'gbk', 'ignore').decode('gbk').strip()
        if n:
          return n
    self.L.exception('\t'.join(names))
    return ''

  def load_conf(self):
    if not isfile(self._conf_file):
      self.L.error('未找到配置文件。')
      return False
    config = ConfigParser()
    config.read(self._conf_file, 'utf8')
    args = config['DEFAULT']
    # root
    if 'root' in args:
      self._root = args['root']
    elif system() == 'Windows':
      self._root = path_join(getenv('APPDATA'),
                            r'Apple Computer\MobileSync\Backup')
    else:
      self.L.error('请设置“备份根目录”')
      return False
    # dest
    try:
      self._dest = expanduser(args['dest']) or None
    except KeyError:
      self._dest = None
    # log
    try:
      self._log_file = expanduser(args['log']) or None
    except KeyError:
      self._log_file = None
    if self._log_file:
      try:
        makedirs(dirname(self._log_file))
      except FileExistsError:
        pass
      handler = FileHandler(self._log_file, encoding='utf8')
      handler.setFormatter(Formatter(
        '%(asctime)s[%(levelname)s]%(filename)s:%(lineno)d(%(funcName)s) %(message)s'))
      self.L.addHandler(handler)
    # compress
    try:
      self._compress = int(args['compress']) or None
    except (KeyError, ValueError):
      self._compress = None
    return True

  def get_mbdb(self):
    def iter_mbdb():
      for f in next(os_walk(self._root))[1]:
        if f != 'Snapshot':
          mbdb = path_join(self._root, f, 'Manifest.db')
          if isfile(mbdb):
            yield mbdb
          else:
            mbdb = path_join(self._root, f, 'Manifest.mbdb')
            if isfile(mbdb):
              yield mbdb
    self.mbdb = iter_mbdb()

  def handle_mbdb(self):
    def iter_mmdb():
      for db in self.mbdb:
        mbdb = self._load_manifest_db(db)
        mmsqlite = defaultdict(lambda: defaultdict(str))
        self.L.info('Finding in %s', dirname(db))
        for offset, fileinfo in mbdb.items():
          if fileinfo['domain'] == 'AppDomain-com.tencent.xin':
            fpath = fileinfo['filename']
            docpath = dirname(fpath)
            fname = basename(fpath)
            if fname == 'MM.sqlite':
              mmsqlite[docpath]['path'] = dirname(db)
              mmsqlite[docpath]['mm'] = fileinfo['filehash']
            elif fname == 'WCDB_Contact.sqlite':
              mmsqlite[docpath]['contacts'] = fileinfo['filehash']
        for k, v in mmsqlite.items():
          self.L.info('Found in %s', k)
          self.L.info(' MM.sqlite:           %s', v['mm'])
          self.L.info(' WCDB_Contact.sqlite: %s', v['contacts'])
          yield k, v['path'], v['mm'], v['contacts']
    self.mmdb = iter_mmdb()

  def parse_mmdb(self):
    def iter_conversation():
      for i, (docpath, path, mm_db, contacts_db) in enumerate(self.mmdb):
        contacts, groups, duplicates = self._load_contacts(path_join(path, contacts_db))
        i = str(i)
        for namehash, chats in self._load_chats(path_join(path, mm_db)):
          messages = deque()
          try:
            uid, mmid, nickname, dispname = contacts[namehash][:4]
          except KeyError:
            uid, mmid, nickname, dispname = '', '', '', '未保存的群' + namehash
          filename = self._get_valid_filename((dispname, nickname, mmid, uid))
          if namehash in duplicates:
            filename += '({})'.format(self._get_valid_filename((mmid, uid)))
          self.L.debug(filename)
          for chat in chats:
            timestamp = strftime('%Y-%m-%d %X', localtime(chat[0]))
            try:
              msgtype = self._get_msg_type(chat[1], chat[3])
            except KeyError:
              self.L.error('Unknown msg type: %d', chat[1])
              msgtype = chat[1]
            direction = self._get_msg_direction(chat[2])
            msg = chat[3].strip()
            sender, s_msg = self._get_sender(msg, contacts)
            s_uid, s_mmid, s_nick, s_disp = sender[:4]
            messages.append((timestamp, msgtype, direction, s_uid or uid,
                s_mmid or mmid, s_nick or nickname, s_disp or dispname,
                s_msg or msg))
          yield i, filename, messages, 'log'
        # Save contacts
        yield i, 'Contacts', contacts.values(), 'contacts'
        # Save groups
        for k, v in groups.items():
          yield i, path_join('Groups', k), v, 'group'
    self.conversations = iter_conversation()

  def save_log(self):
    for i, filename, messages, category in self.conversations:
      if not self._dest:
        continue
      if category == 'log':
        header = ('时刻', '消息类型', '消息方向', 'ID', '微信号', '昵称', '显示名称', '内容')
        fname = filename
      elif category == 'contacts':
        header = ('ID', '微信号', '昵称', '备注', '国', '省', '市', '签名')
        fname = path_join(filename, 'contacts')
      elif category == 'group':
        header = ('ID', '微信号', '昵称', '备注', '国', '省', '市', '签名')
        fname = filename
      fpath = path_join(self._dest, i, fname)
      try:
        makedirs(dirname(fpath))
      except FileExistsError:
        pass
      if self._compress:
        fo = bz2_open(fpath + '.csv.bz2', 'wt', encoding='utf8')
      else:
        fo = open(fpath + '.csv', 'w', encoding='utf8')
      wt = csv_writer(fo)
      wt.writerow(header)
      wt.writerows(messages)
      fo.close()


if __name__ == '__main__':

  wechat = Wechat()

  # 载入配置文件
  if wechat.load_conf():

    # 找到 Manifest.mbdb
    wechat.get_mbdb()

    # 找到 MM.sqlite
    wechat.handle_mbdb()

    # 解析 MM.sqlite
    wechat.parse_mmdb()

    # 保存到文件
    wechat.save_log()
