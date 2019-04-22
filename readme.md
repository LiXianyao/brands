1、文件夹说明：
        |flask                  #目前所用的http服务器的代码所在文件夹
            |data               #商标相似度计算时产生的缓存文件
        
        |dataStorage            #处理csv到数据库的代码，redis连接的部分
        |processdata            #处理csv到数据库的代码，mysql连接的部分

        |similarity             #学姐写的相似度特征值计算的代码

        |train                  #模型有关的代码：训练数据构造、模型训练、模型使用等
            |models             #存放训练出的模型、对应的参数记录
            |data               #存放构造出的训练数据（含商标名等），以及训练数据转换为模型要求的输入格式的中间文件
            |testRes            #存放模型训练代码里，测试集对每行数据的测试结果

2、使用说明
·包括训练、使用服务器测试，还有多进程处理响应请求

        2、1）训练
            I、生成训练数据
                所用文件：/train文件夹下的form_train_data_IV.py
                使用前准备：在form_train_data_redis（）函数里，修改如下变量：
                    ·file_name = range(a,b)             ##其实就是从a.csv到b.csv里面获取数据
                    ·csv_name = u"train_7_4_1.csv"      ##训练数据保存出来的文件名（个人习惯按日期命名）
                    ·再往下还有个地方写了 with open("/home/lab/brands/csv3/" + file_name, "rU") as csv_file:  这里要把csv数据文件对应的路径改上去
                    ·gate = ['C','C','C','C', 0.9, 0.8, 0.8, 'C', 'C',1.0]
                            ##对应了给每种特征值设置的阈值，即gate[i]是第i个特征的阈值，当特征值>=阈值的时候被选为训练数据
                            ##'C'是指，有个根据输入商标长度的特判，对于长度<4的商标和长度>=4的商标做了不同阈值，详见compute_similar函数
                    ·构造训练数据过程如下：
                        a)先根据输入商标的拼音，取每个读音对应的商标名集合
                              把输入商标的拼音按最小数量，做组合，比如bai shi ke le,最后就取[bai shi ke]等三元组，对应的拼音商标名的交集。再取并集
                        b)调用similarity中的brand里的函数，计算特征值
                        c)根据gate (与构建训练数据时的解释相同)，筛选数据，把满足gate条件的数据存入csv_name指定的文件
                    ·最后在最底下，if  __name__=="__main__":里，有一句 lowb , num = 0, 10000  这样的，
                        设置的是0类和1类的商标取多少个（每一类从扫描csv遇到的第0个合法的数据行，到第10000个，是标0和标1各10000个哦！）
                改完这些变量直接跑就行。
            II、训练数据转模型输入数据
                所用文件：/train文件夹下的trans_train_data_II.py，配置文件train.config
                使用前准备：
                    ·修改train.config文件 中的train_set 字段，给I中跑出来的csv路径。
                    ·train.config 中的ratio字段，是训练数据和测试数据的划分比例， 0.7表示csv里每一类数据（数量不等时取最小值）的70%做训练，剩下的做测试
                    ·如果I中跑出来的csv的格式有变动，就改trans_train_data_II.py里的 train_Data（）中的解析部分吧。。
                        （csv没变动的时候只改配置文件就可以了）
                    ·其他的字段是模型参数但这边没用上，因为是拿的以前写给另一个项目的配置文件（先别删，可能以后要用）
                修改好配置文件后，运行trans_train_data_II.py -i ‘一个版本编号’  这个是编号也会变成之后对应的模型里编号的一部分，不打的话就是时间戳
                    运行后，会在data/input中产生几个以'版本编号'为前缀的输入文件，做后续使用
            III、模型训练
                所用文件：/train文件夹下的classify_xgboost_train.py，配置文件train_models.config
                使用前准备：
                    ·修改train_models.config文件, [action_parameter]中time_stamp 填II里面-i选项后的编号，model_id随便加个编号
                        比如time_stamp = 180703022，model_id = N0003，就是使用前缀为180703022的文件，跑出来的模型就叫‘180703022_N0003.models'
                        目的就是方便调参。。
                    ·train_models.config文件的[boost_parameter]和[train_parameter]都是超参数的设置，其余的暂时没用
                修改好配置文件后，运行classify_xgboost_train.py脚本，等它跑完，会在train/testRes里有个csv是测试数据的对比结果
                        ，在train/models/里产生.model模型文件和.parameter使用的参数记录

        2、2）flask在线后台(未修改，还是旧版)
            ·只用了flask_server.py，服务调用的函数都在train/文件夹里，名字里带pre或者prediction的
                直接运行flask_server.py，服务器的主要用处是将对redis中的商标，按所含字的拼音构成的集合驻留内存，节约测试的启动时间。
                提供五个接口：
                ·@app.route('/predict/allRes', methods=['POST'])
                ·@app.route('/predict/onlyName', methods=['POST'])          ##只对请求中指定的大类查询，两个接口分别是简略结果和全结果

                ·@app.route('/predictAll/onlyName', methods=['POST'])
                ·@app.route('/predictAll/allRes', methods=['POST'])         ##对所有的大类查询，两个接口分别是简略结果和全结果

                ·@app.route('/reload/prediction', methods=['POST'])         #reload train/form_pre_data_IV_flask.py

                收到的查询请求，会调用train/form_pre_data_IV_flask.py 进行处理，类似构造训练数据的过程，
            ·先根据输入商标的拼音，取每个读音对应的商标名集合
                  把输入商标的拼音按最小数量，做组合，比如bai shi ke le,最后就取[bai shi ke]等三元组，对应的拼音商标名的交集。再取并集
            ·调用similarity中的brand里的函数，计算特征值
            ·根据gate (与构建训练数据时的解释相同)，筛选数据
            ·筛选后的特征数据依次调用trans_pre_data.py -> classify_xgboost_prediction.py，接受返回结果，构建结果字典

3、部署说明
·需要安装的python环境包
    ·可以直接pip 安装的：redis,flask, jieba, pypinyin, python-Levenshtein, synonyms
    ·需要git下载编译安装：xgboost
