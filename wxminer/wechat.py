#!/usr/bin/evn python
# -*- coding: utf-8 -*-
"""
WX Miner: Deep into WeChat

Copyright (c) 2021-2023 xleven
"""

import hashlib
import logging
import re
import sqlite3
from datetime import datetime, timedelta
from functools import partial

import pandas as pd
from ibackupy import Backup
from lxml import etree

logger = logging.getLogger(__name__)


class WeChat:

    """
    Different types of constants:
        0. general consts
        1. RE_xx, used to extract infos
        2. MY_xx, prefix with `Documents/[mymd5]/` to get exact path

    Plural names indicate possible multiple matches
    """
    APP = "com.tencent.xin"
    RE_USERNAME = "[0-9A-Za-z_-]{6,20}"
    RE_CHATROOM = "[0-9]{8,12}@chatroom"
    RE_MMSETTING_PATHS = f"Documents/MMappedKV/mmsetting.archive.({RE_USERNAME})"
    RE_USER_NICKNAME = b"88[\x00-\x2f]{2}(.*?)\x01"
    RE_USER_HEADIMG = b"headimgurl.*(https?://.*?/.*?/(?:.*?/)?.*?/[0-9]+)"
    RE_CONTACT_HEADIMG = b"https?://.+?/.+?/(?:.+?/)?.+?/[0-9]+"
    RE_FRIEND_GENDER = b"\x08([\x00-\x02])"
    RE_GROUP_FOUNDER = f"\x12.({RE_USERNAME})".encode()
    RE_GROUP_CHATROOM = b"<RoomData>.+</RoomData>"
    RE_MESSAGE_TABLE = b"(Chat_[a-z0-9]{32})"
    RE_MESSAGE_SPLIT = (
        f"^(?:(?P<sender>{RE_USERNAME}|{RE_CHATROOM}):\\n)?(?P<Message>.+)$")
    MY_CONTACT_DB = "DB/WCDB_Contact.sqlite"
    MY_MESSAGE_DB = "DB/message_{}.sqlite"
    MY_SESSION_DB = "session/session.db"
    MSG_TYPE = {
        "text": 1,
        "image": 3,
        "audio": 34,
        "bigheadimg": 42,
        "video": 43,
        "emoji": 47,
        "location": 48,
        "appmsg": 49,
        "system": 10000,
        "sysmsg": 10002,
    }

    def __init__(self,
                 backup_dir: str = "",
                 device_udid: str = "",
                 username: str = "",
                 ) -> None:
        """
        Initialize instance with given backup, device and username

        Parameters
        ----------
        backup_dir: str
            iTunes backup directory path, leave blank if you don't know
        """
        backup = Backup(path=backup_dir)
        backup.set_device(udid=device_udid)
        self.files = {
            file["relativePath"]: file["path"]
            for file in backup.get_files(app="com.tencent.xin", real_path=True)
        }
        if username:
            self.set_user(username)

    @staticmethod
    def _username_to_md5(username: str) -> str:
        return hashlib.md5(username.encode('utf-8')).hexdigest()

    @staticmethod
    def _parse_blob(blob, regex) -> str:
        matches = re.findall(regex, blob, re.DOTALL)
        if len(matches) > 0:
            result = matches[0].decode()
        else:
            result = ""
        return result

    @staticmethod
    def _parse_xml(xml, path: str = "", attr: str = "") -> str:
        """
        Try hard to extract info from XML with given path or attr
        """
        xpath = "./"
        if path:
            xpath += "/{}".format(path)
        if attr:
            xpath += "/@{}".format(attr)
        else:
            xpath += "/text()"
        try:
            tree = etree.XML(xml, parser=etree.XMLParser(
                remove_blank_text=True, recover=True))
            t = str(tree.xpath(xpath)[0])
        except Exception:
            t = ""
        return t

    def _get_user_info(self, username: str = "") -> tuple:
        """
        Parse  user info in mmsetting.archive blob file
        """
        if not username:
            username = self.username
        ptn = re.sub(r"\(.*\)", username, self.RE_MMSETTING_PATHS)
        logger.debug(f"Parsing user info from {ptn}")
        mmsetting = self.files[ptn].read_bytes()
        try:
            name = re.findall(self.RE_USER_NICKNAME, mmsetting)[0].decode()
        except Exception as err:
            logger.warning(f"Fail to parse nickname: {err}")
            name = "微信用户"
        try:
            headimg = re.findall(self.RE_USER_HEADIMG, mmsetting)[0].decode()
        except Exception as err:
            logger.warning(f"Fail to parse headimg: {err}")
            headimg = ""
        return name, headimg

    def get_user_list(self) -> list:
        ptn = re.compile(self.RE_MMSETTING_PATHS)
        user_list = list(set(
            re.findall(ptn, file)[0] for file in self.files if re.match(ptn, file)
        ))
        assert len(user_list) > 0, "No user found!"
        return user_list

    def set_user(self, username: str = "") -> str:
        user_list = self.get_user_list()
        if len(user_list) > 1:
            if username and username not in user_list:
                logger.warning(
                    "Given user not found! Select first one instead")
                username = user_list[0]
        else:
            logger.info("Only one user found, input ignored!")
            username = user_list[0]

        self.username = username
        self.nickname, self.headimg = self._get_user_info()

        self.mymd5 = self._username_to_md5(self.username)
        self.prefix = f"Documents/{self.mymd5}/"

        self.message_dbs = self._get_message_db_list()
        self.message_tables = self._get_message_tables()

        self.get_contact()

        return username

    def _get_message_db_list(self) -> list:
        ptn = self.prefix + self.MY_MESSAGE_DB
        i, dbs = 1, []
        while ptn.format(i) in self.files:
            dbs.append(ptn.format(i))
            i += 1
        return dbs

    def _get_message_db_tables_by_seq(self, db) -> list:
        """
        Read `sqlite_sequence` table in message_db to get chat table list

        Note
        ----
        The `sqlite_sequence` table may only occurs in primary device,
        which indicates your last seen position in chat.
        """
        db_path = self.files[db]
        sql = """
            SELECT name
            FROM sqlite_sequence
        """
        try:
            with sqlite3.connect(db_path) as conn:
                csor = conn.execute(sql)
                tables = [table[0] for table in csor.fetchall()]
        except Exception as err:
            logger.warning(f"Failed to read sequence tables in {db}: {err}")
            tables = []
        return tables

    def _get_message_db_tables_by_ddl(self, db) -> list:
        """
        Extract `Chat_xxx`-like names from `message_1.sqlite-first.material`

        Note
        ----
        The `*-first|last.` material blob files seem like DDLs of WCDB
        """
        material = db + "-first.material"
        try:
            material_path = self.files[material]
            ddl = material_path.read_bytes()
            tables = list(set(re.findall(self.RE_MESSAGE_TABLE, ddl)))
        except Exception as err:
            logger.warning(f"Failed to read ddl tables in {material}: {err}")
            tables = []
        return tables

    def _get_message_db_tables(self, db) -> list:
        tables = (self._get_message_db_tables_by_seq(db)
                  or self._get_message_db_tables_by_ddl(db))
        return tables

    def _get_message_tables(self) -> dict:
        tables = {
            tb: self.files[db]
            for db in self.message_dbs
            for tb in self._get_message_db_tables(db)
        }
        return tables

    def get_contact(self):
        logger.debug("Parsing WeChat contacts")
        contact_db = self.files[self.prefix + self.MY_CONTACT_DB]
        sql_friends = """
            SELECT userName
                ,type
                ,dbContactRemark
                ,dbContactHeadImage
                ,dbContactProfile
            FROM Friend
            WHERE type % 2 = 1
                AND dbContactEncryptSecret NOT NULL
        """
        sql_groups = """
            SELECT userName
                ,type
                ,dbContactRemark
                ,dbContactChatRoom
            FROM Friend
            WHERE userName LIKE "%@chatroom"
        """
        with sqlite3.connect(contact_db) as conn:
            friends = pd.read_sql(sql_friends, conn, index_col="userName")
            groups = pd.read_sql(sql_groups, conn, index_col="userName")

        remark_fields = {
            10: "nickname",
            18: "id_new",
            26: "alias",
            34: "alias_pinyin",
            42: "alias_PY",
            50: "nickname_pinyin",
            58: "description",
            66: "tag",
        }

        def parse_remark(blob) -> pd.Series:
            csor, data = 0, {}
            while csor < len(blob):
                dtype = blob[csor]
                csor += 1
                step = blob[csor]
                csor += 1
                data[dtype] = blob[csor:csor+step].decode()
                csor += step
            remark = pd.Series(data).rename(remark_fields)
            return remark

        parse_headimg = partial(
            self._parse_blob, regex=self.RE_CONTACT_HEADIMG)
        parse_profile = partial(self._parse_blob, regex=self.RE_FRIEND_GENDER)

        parse_founder = partial(
            self._parse_blob, regex=self.RE_GROUP_FOUNDER)

        def parse_chatroom(blob_chatroom) -> pd.DataFrame:
            xml = self._parse_blob(
                blob_chatroom, regex=self.RE_GROUP_CHATROOM)
            try:
                chatroom = pd.read_xml(
                    xml, xpath="Member").set_index("UserName")
            except Exception:
                chatroom = pd.DataFrame()
            return chatroom

        try:
            logger.debug("Parsing friends out of contacts")
            friends = friends.join(
                friends["dbContactRemark"].apply(parse_remark))
            friends["headimg"] = friends["dbContactHeadImage"].map(
                parse_headimg)
            friends["gender"] = friends["dbContactProfile"].map(parse_profile)
            friends["table"] = "Chat_" + \
                friends.index.map(self._username_to_md5)
        except Exception as err:
            logger.warning(f"Failed due to {err}")
        else:
            self.friends = friends.drop(
                columns=friends.filter(like="dbContact").columns)

        try:
            logger.debug("Parsing groups out of contacts")
            groups = groups.join(groups["dbContactRemark"].apply(parse_remark))
            groups["founder"] = groups["dbContactChatRoom"].map(parse_founder)
            groups["chatroom"] = groups["dbContactChatRoom"].map(
                parse_chatroom)
            groups["table"] = "Chat_" + groups.index.map(self._username_to_md5)
        except Exception as err:
            logger.warning(f"Failed due to {err}")
        else:
            self.groups = groups.drop(
                columns=groups.filter(like="dbContact").columns)

    def _read_chat_message(self,
                           contact: str,
                           sdate: str = "",
                           edate: str = ""):
        table = "Chat_{}".format(self._username_to_md5(contact))
        cols = [
            "CreateTime",
            "Des",
            "ImgStatus",
            "MesLocalID",
            "Message",
            "MesSvrID",
            "Status",
            "TableVer",
            "Type",
        ]
        sql = f"""
            SELECT {",".join(cols)}
            FROM {table}
        """
        try:
            ed = datetime.fromisoformat(edate)
        except Exception:
            ed = datetime.now()
        finally:
            ed = ed.replace(hour=0, minute=0, second=0, microsecond=0)
            et = int((ed+timedelta(days=1)).timestamp())
            sql += f" WHERE CreateTime < {et}"

        if sdate:
            try:
                sd = datetime.fromisoformat(sdate)
            except Exception:
                logger.warning("Wrong date format, check your input!")
            else:
                sd = sd.replace(hour=0, minute=0, second=0, microsecond=0)
                st = int(sd.timestamp())
                sql += f" AND CreateTime >= {st}"

        try:
            database = self.message_tables[table]
            with sqlite3.connect(database) as conn:
                msg = pd.read_sql(sql, conn)
        except Exception as err:
            logger.error(f"Failed to read message of {contact}: {err}")
            msg = pd.DataFrame(columns=cols)
        return msg

    def _parse_videomsg(self, msg: pd.DataFrame) -> pd.DataFrame:
        sender = msg["Message"].apply(
            self._parse_xml, path="videomsg", attr="fromusername")
        parsed = pd.DataFrame({"sender": sender})
        return parsed

    def _parse_appmsg(self, msg: pd.DataFrame) -> pd.DataFrame:
        sender = msg["Message"].apply(
            self._parse_xml, path="fromusername"
        ).combine_first(
            msg["Message"].apply(self._parse_xml, path="fromUser")
        )
        text = msg["Message"].apply(self._parse_xml, path="title")
        msgtype = (
            msg["Message"].apply(self._parse_xml, path="type")
                          .replace("", pd.NA)
                          .fillna(self.MSG_TYPE["appmsg"])
                          .astype(int)
        )
        parsed = pd.DataFrame(
            {"sender": sender, "text": text, "type": msgtype})
        return parsed

    def _parse_chat_message(self, msg: pd.DataFrame,
                            contact: str,
                            replace_media_with_text=True) -> pd.DataFrame:
        """
        Clean chat message
        """
        cols = ["dt", "sender", "text", "type"]
        msg = msg.reindex(columns=(msg.columns.tolist() + cols))
        msg["dt"] = msg["CreateTime"].map(datetime.fromtimestamp)

        idx = {k: msg["Type"] == v for k, v in self.MSG_TYPE.items()}
        idx["mymsg"] = msg["Des"] == 0

        # split sender and message when parsing group chat
        if re.match(self.RE_CHATROOM, contact):
            msg.loc[~idx["mymsg"], ["sender", "Message"]] = (
                msg.loc[~idx["mymsg"], "Message"].str.extract(
                    pat=self.RE_MESSAGE_SPLIT, flags=re.DOTALL, expand=True))

        # parsing
        videomsg_parsed = self._parse_videomsg(msg[idx["video"]])
        appmsg_parsed = self._parse_appmsg(msg[idx["appmsg"]])

        # fill sender
        msg.loc[idx["mymsg"], "sender"] = self.username
        msg.loc[idx["video"], "sender"] = videomsg_parsed["sender"]
        msg.loc[idx["appmsg"], "sender"] = appmsg_parsed["sender"]
        msg.loc[(msg["Type"]<10000)&(msg["sender"].isna()), "sender"] = contact

        # fill type
        msg["type"] = msg["Type"]
        msg.loc[idx["appmsg"], "type"] = appmsg_parsed["type"]

        # fill text
        msg.loc[idx["text"], "text"] = msg.loc[idx["text"], "Message"]
        msg.loc[idx["appmsg"], "text"] = appmsg_parsed["text"]
        if replace_media_with_text:
            msg.loc[idx["image"], "text"] = "[图片]"
            msg.loc[idx["audio"], "text"] = "[语音]"
            msg.loc[idx["video"], "text"] = "[视频]"
            msg.loc[idx["emoji"], "text"] = "[表情]"

        return msg[cols]

    def get_friend_chat(self, friend: str,
                        st: str = "", et: str = "",
                        raw=False) -> pd.DataFrame:
        msg = self._read_chat_message(friend, st, et)
        if raw:
            return msg
        msg = self._parse_chat_message(msg, friend)
        return msg

    def get_group_chat(self, group: str,
                       st: str = "", et: str = "",
                       raw=False,
                       use_display_name=True) -> pd.DataFrame:
        msg = self._read_chat_message(group, st, et)
        if raw:
            return msg
        msg = self._parse_chat_message(msg, group)
        names = self.friends["nickname"].copy()
        names[group] = self.groups["nickname"]
        names[self.username] = self.nickname
        if use_display_name:
            member = self.groups.at[group, "chatroom"]
            if "DisplayName" in member:
                names = member["DisplayName"].combine_first(names)
        msg["name"] = msg["sender"].map(names)
        return msg

    def get_session(self) -> pd.DataFrame:
        sql = """
            SELECT UsrName
                ,CreateTime
                ,unreadcount
            FROM SessionAbstract
        """
        try:
            session_db = self.files[self.prefix + self.MY_SESSION_DB]
            with sqlite3.connect(session_db) as conn:
                sessions = pd.read_sql(sql, conn)
        except Exception:
            sessions = pd.DataFrame()
        return sessions
