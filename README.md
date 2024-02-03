# 安装mysql
这里我用docker安装最新版本
# 克隆项目
```
git clone https://github.com/octavei/dota-indexer.git
```
# 创建python虚拟环境并激活
```angular2html
python3 -m venv myenv
source myenv/bin/activate
```
# 安装依赖
```angular2html
pip install -r requirements.txt
```
# 修改配置

```angular2html
cp .env.example .env
```
修改对应环境变量

# 运行索引器
```angular2html
python indexer.py
```


