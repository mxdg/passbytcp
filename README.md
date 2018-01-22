# passbytcp
# 目的
网上搜索了不少tcp内网穿透的代码，功能全的大多是go版本，无奈对go不感冒。<br>  
后来一直搜索python的tcp内网穿透代码，版本很少，而且大多有各种缺陷，后来搜索到[shootback](https://github.com/aploium/shootback)，运行比较稳定。<br>  
但想加入更多功能，所以重新改造了代码，实现了更多功能。<br>  
# 功能介绍
通过外网vps在任意地方访问没有公网独立ip的电脑，树莓派等等<br>  
使内网网站能被公网访问<br>  
实现域名访问内网指定网站<br>  
给域名访问的用户进行简单http授权验证<br>  
方便的在本地调试支付接口<br>  
动态识别服务端配置，避免重启进程<br>  
各种内网tcp暴露到公网，比如在手机上vnc登录家里的树莓派桌面。<br>  

# 配置说明
服务端需要python3版本<br>
客户端可以使用python2和3版本【如果python2使用有问题，请用python3启动客户端】<br>
服务端配置文件config.json<br>
*server文件夹是公网服务器使用的<br>
*slaver文件夹是内网服务器使用的<br>
<br>
# 特别说明
服务器配置为10秒检测一次。<br>
如果tcp配置项需要修改，请先删除目标配置项，等服务器检测删除端口后，再添加配置项进去。<br>
如果是tcp新增或者删除，则可以直接修改json配置。<br>
http的customer，to_master如果要修改，需要重新运行服务端配置。<br>
http的域名验证，域名访问可以随时修改，随时生效<br>
#### http为域名配置
to_master:过公网服务器哪个端口进行域名转发,端口必须在tcp配置组里存在<br>
customer:域名请求哪个端口<br>
host数组项目::可以访问的域名，和对应域名的授权帐号密码<br>
#### tcp为域名配置
master:公网服务器提供给内网服务器使用的端口<br>
customer:公网访客通过这个接口可以访问连接master的内网服务器端口<br>
secretkey:内网服务器连接master需要使用的密钥<br>
```javascript
config.json
{
    "http":{
        "to_master":"0.0.0.0:10001",
        "customer":"0.0.0.0:80",
        "host":[
            {
                "domain":"pwd.yourdomain.com",
                "auth":{
                    "username":"cncn",
                    "password":"1234"
                }
            },
            {
                "domain":"nopwd.yourdomain.com"
            }
        ]
    },
    "tcp":[
    {
        "master":"0.0.0.0:10001",
        "customer":"0.0.0.0:10101",
        "secretkey":"pwd001"
    },
	{
        "master":"0.0.0.0:10002",
        "customer":"0.0.0.0:10102",
        "secretkey":"pwd002"
    }
] 
}
````
#### 运行
配置好服务器config.json后
服务在进入server文件夹，执行
```python
python3 server.py
或者后台运行
nohup python3 server.py >log_server.log 2>&1 &
```
客户端进入slaver文件夹，执行
```
python slaver.py -m 公网IP:端口 -t 内网IP:端口 -k 密钥

转发80，http端口，需要在http配置中配置对应域名信息
python slaver.py -m 123.123.123.123:10001 -t 127.0.0.1:80 -k pwd001
或者
nohup python slaver.py -m 123.123.123.123:10001 -t 127.0.0.1:80 -k pwd001>log_10001.log 2>&1 &

转发22，ssh端口
python slaver.py -m 123.123.123.123:10002 -t 127.0.0.1:22 -k pwd002
或者
nohup python slaver.py -m 123.123.123.123:10002 -t 127.0.0.1:22 -k pwd002>log_10002.log 2>&1 &

```

