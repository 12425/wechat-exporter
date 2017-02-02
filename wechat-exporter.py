#!/usr/bin/env python3
# vim: fileencoding=utf-8

from os import makedirs, getenv, walk as os_walk
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
  offset += 2 # value x05 x00, not sure what this is
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

  def _load_contacts(self, db):
    with Sqlite(db) as con:
      tables = con.get_query('SELECT userName, dbContactRemark FROM Friend')
      contacts = {}
      for user, remark in tables:
        namehash = md5(user.encode('utf8')).hexdigest()
        mmid, nickname, dispname = self._parse_name(remark)
        contacts[namehash] = (mmid or user, nickname, dispname)
      return contacts

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
      if remark[offset] == 0x1a: # custom dispname
        offset += 1
        length = remark[offset]
        offset += 1
        dispname = remark[offset:offset + length].decode('utf8')
        if mmid and dispname:
          break
        offset += length
      elif remark[offset] == 0x12: # wechat name
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

  def _get_msg_type(self, tp):
    return {
      1: '文本',
      3: '图片',
      34: '语音',
      42: '名片',
      43: '视频',
      44: '视频',
      47: '表情',
      48: '位置',
      49: '链接',
      62: '视频',
      10000: '系统消息',
    }[tp]

  def _get_msg_direction(self, des):
    return {
      1: '接收',
      0: '发送',
    }[des]

  def _get_sender(self, msg, contacts):
    sender = msg.split(':\n', 1)
    if len(sender) == 2:
      namehash = md5(sender[0].encode('utf8')).hexdigest()
      try:
        return list(contacts[namehash]) + [sender[1]]
      except KeyError:
        pass
    return None

  def _get_valid_filename(self, names):
    for name in names:
      if name:
        n = self._fn_pat.sub('', name).encode(
            'gbk', 'ignore').decode('gbk').strip()
        if n:
          return n
    L.exception('\t'.join(names))
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
      self._compress = args['compress'] or None
    except KeyError:
      self._compress = None
    return True

  def get_mbdb(self):
    def iter_mbdb():
      for f in next(os_walk(self._root))[1]:
        if f != 'Snapshot':
          mbdb = path_join(self._root, f, 'Manifest.mbdb')
          if isfile(mbdb):
            yield mbdb
    self.mbdb = iter_mbdb()

  def handle_mbdb(self):
    def iter_mmdb():
      for db in self.mbdb:
        mbdb = process_mbdb_file(db)
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
        contacts = self._load_contacts(path_join(path, contacts_db))
        for namehash, chats in self._load_chats(path_join(path, mm_db)):
          messages = deque()
          try:
            mmid, nickname, dispname = contacts[namehash]
          except KeyError:
            mmid, nickname, dispname = '', '', '未保存的群' + namehash
          filename = self._get_valid_filename((dispname, nickname, mmid))
          self.L.debug(filename)
          for chat in chats:
            timestamp = strftime('%Y-%m-%d %X', localtime(chat[0]))
            try:
              msgtype = self._get_msg_type(chat[1])
            except:
              self.L.error('Unknown msg type: %d', chat[1])
              msgtype = chat[1]
            direction = self._get_msg_direction(chat[2])
            msg = chat[3].strip()
            sender = self._get_sender(msg, contacts)
            if sender is not None:
              s_mmid, s_nick, s_disp, s_msg = sender
              messages.append((timestamp, msgtype, direction,
                  s_mmid, s_nick, s_disp, s_msg))
            else:
              messages.append((timestamp, msgtype, direction,
                  mmid, nickname, dispname, msg))
          yield str(i), filename, messages
    self.conversations = iter_conversation()

  def save_log(self):
    for i, filename, messages in self.conversations:
      if not self._dest:
        continue
      try:
        makedirs(path_join(self._dest, i))
      except FileExistsError:
        pass
      if self._compress:
        fo = bz2_open(path_join(self._dest, i, filename + '.csv.bz2'),
             'wt', encoding='utf8')
      else:
        fo = open(path_join(self._dest, i, filename + '.csv'),
             'w', encoding='utf8')
      wt = csv_writer(fo)
      wt.writerow(('时刻', '消息类型', '消息方向', '微信号', '昵称', '显示名称', '内容'))
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
