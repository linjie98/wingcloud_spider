#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author   : linjie
import logging

import pypinyin

# 中文和拼音转换的工具类
# 不带声调的(style=pypinyin.NORMAL)
class ZhSwitch:
    # 日志配置
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s] %(levelname)s [%(funcName)s: %(filename)s, %(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    #中拼转换
    def zh_pin(word):
        s = ''
        for i in pypinyin.pinyin(word, style=pypinyin.NORMAL):
            s += ''.join(i)+"_"
        #切片去掉最后一个"_"符号
        str = s[:-1]
        logging.info('{}'.format(str))
        return str

if __name__ == '__main__':
    a = ZhSwitch
    #print(a.zh_pin("手机壳地方"))
    a.zh_pin("sdsd时代大厦")