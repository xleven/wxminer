# WX Miner

> Deep into WeChat

With an iTunes backup, wxminer helps you delivering a full analysis of your WeChat data including sessions, contacts, chats and all.

## Install

```Shell
$ pip install wxminer
```

## Usage

Say if you want to export a friend chat:

```Shell
$ python -m wxminer --user "user_id" \
                    --friend "friend_id" \
                    --sdate "2023-01-01" \
                    --output "chat.csv"
```

or you prefer a pandas dataframe:

```Python
from wxminer import WeChat

wx = WeChat("user_id")
df_chat = wx.get_friend_chat("friend_id", st="2023-01-01")
```

## Roadmap

- [x] chats export
- [ ] session export
- [ ] moments export
- [ ] daily dashboard
- [ ] annual report
- [ ] chat mining

## License

[MIT](./LICENSE)
