import argparse
import logging

from .wechat import WeChat

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--loglevel", default="info")
    parser.add_argument("-u", "--username", default="")
    parser.add_argument("-f", "--friend")
    parser.add_argument("-s", "--sdate")
    parser.add_argument("-e", "--edate")
    parser.add_argument("-o", "--output")
    args = parser.parse_args()

    logging.basicConfig(format='[%(levelname)s - %(asctime)s] %(message)s')
    logger = logging.getLogger()
    logger.setLevel(args.loglevel.upper())

    user = args.username
    friend = args.friend
    st, et = args.sdate, args.edate
    out = args.output

    wx = WeChat()
    wx.set_user(user)
    chat = wx.get_friend_chat(friend, st=st, et=et)
    if out:
        chat.to_csv(out)
    else:
        print(chat)