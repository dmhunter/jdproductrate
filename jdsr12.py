#-*- coding: UTF-8 -*-
import sys
import os
import urllib
import urllib2
import json 
import lxml
import xmllib
import chardet
import bs4
from bs4 import BeautifulSoup
import shutil
reload(sys)
sys.setdefaultencoding('utf-8')
import jieba
import math
import time
import random
import numpy as np
from lxml import etree
from lxml import html
import chardet
import re
import MySQLdb
#'Connection':'keep-alive','Cookie':'cm_sg_cache=|0;pgv_info=ssid=s9440602665; pgv_pvid=7619944302; cm_cache=|0','Host':'cm.e.qq.com'
def firstclasspro():
#首页一级大类产品名以及子页面链接获取
	indexurl='http://www.jd.com'
	indexpagerequ=urllib2.Request(indexurl,headers=header)
	indexpageresp=urllib2.urlopen(indexpagerequ).read()
#	print type(indexpageresp)
	encode=chardet.detect(indexpageresp)['encoding']
	indexpagexpath=etree.HTML(indexpageresp.decode(encode))
#	print indexpagexpat
	firstclassprodict=dict()
#	print indexpagexpath.xpath("//div[@id='categorys-2014']")
	for i in indexpagexpath.xpath("//div[@id='categorys-2014'][1]/div[2]/div[1]//div"):
		for j in i.xpath("//h3[1]//a"):
			firstclassprodict.update({j.xpath("text()")[0]:j.xpath("@href")[0]})
#	print len(firstclassprodict)
	print "一级大类的url已全部获取……"
	return firstclassprodict
#返回数据结构为｛类别名称1：页面url1，类别名称2:页面url2...｝

def secondclasspro(firstclassprodict):
#根据一级大类中获取的url，获取二级商品类别中的子产品类别
	suburllist=[]
	if os.path.exists(r"urltxts/"):
		shutil.rmtree(r"urltxts/")
	else:
		pass
	os.mkdir(r"urltxts/")
	for i in firstclassprodict:
		try:
			suburllist+=urlpageget(firstclassprodict[i])
		except:
			print "从链接",i,"获取list开头的url失败，继续下一个……"
			continue	
		try:	
			writetxt(i,urlpageget(firstclassprodict[i]))
		except:
			print "从链接",i,"获取list开头的url写入文件时发生错误，继续下一个……"
			continue
	urltxtsdir=os.listdir(r"urltxts/")
	urllist=list(set(suburllist))
	print "二级大类的url已全部获取，详情请查看本地文件夹urltxts下的各个txt文件"
	return urllist,urltxtsdir

def preprocess(urls,txtorlist='t'):
#对所有生成的txt文件或列表中保存的url链接（即函数secondclasspro的返回值）进行预处理，最后我们需要的是以list开头的url,txtorlist表示传入的参数形式，分为列表和文档
	if urls:
		if txtorlist=='t':
			targeturls=list()
			tobeprocessurls=list()
			targeturlstxt=open('targeturlstxt.txt','wa')
			for i in urls:
				urlfile=open(i,'r')
				for j in urlfile:
					if re.match(r'http://list.jd.com/',j):
						targeturls.append(j)
						targeturlstxt.write(j)
					else:
						tobeprocessurls.append(j)
			targeturlstxt.close()
			return targeturlstxt,targeturls,tobeprocessurls
		if txtorlist=='l':
			targeturls=list()
			tobeprocessurls=list()
			for i in urls:
				if re.match(r'http://list.jd.com/',i):
					targeturls.append(i)
				elif re.match(r'http://',i):
	#二级子页面中有些url虽然不是list或search开头，但是在下一级页面中有我们的目标url，需要抓取
					tobeprocessurls.append(i)
				else:
					pass
			print "目标url已获取……"
			return targeturls,tobeprocessurls
		else:
			print "参数错误！"
			return False
	else:
		return [],[]

def dbcreate():
	if "JD" in getdatabasenames():
		print "数据库JD已经存在"
	else:
		print "数据库JD正在创建……"
		try:
			db=MySQLdb.connect(host='localhost',port=3306,user='root',passwd='hsj123',charset='utf8')
			curs=db.cursor()
			curs.execute("create database JD DEFAULT CHARSET utf8 COLLATE utf8_general_ci;")
			db.commit()
			print '数据库JD已创建成功!'
			curs.close()
			db.close()
		except:
			print '数据库JD创建失败!'

def dbtablenullornot(tablename):
#查询数据库一个表是否为空，如果为空，则返回false，否则返回true
	db=MySQLdb.connect(host='localhost',port=3306,db='JD',user='root',passwd='hsj123',charset='utf8')
	curs=db.cursor()
	curs.execute("select count(*) from %s")%tablename
	rows=curs.fetchone()
	db.commit()
	if rows[0]==0:
		return False
	else:
		return True
	curs.close()
	db.close()


def getdbtablenames():
#获取数据库中已经存在的表，返回表名列表
	try:
		dbconnect=MySQLdb.connect(host='localhost',user='root',passwd='hsj123',db='JD',charset='utf8')
		dbcurs=dbconnect.cursor()
		dbcurs.execute("show tables;")
		dbconnect.commit()
		result=dbcurs.fetchall()
		existedtables=[]
		for i in result:
			existedtables.append(i[0])
	#注：result结果是一个元组列表
		dbcurs.close()
		dbconnect.close()
		return existedtables
	except:
		print "获取数据库表时发生错误！"


def getdatabasenames():
#为判断数据库jd是否已存在需要先找出mysql中已有的数据库
	db=MySQLdb.connect(host='localhost',port=3306,user='root',passwd='hsj123',charset='utf8')
	curs=db.cursor()
	curs.execute("show databases;")
	db.commit()
	result=curs.fetchall()
	databases=[]
	for i in result:
		databases.append(i[0])
	return databases


def itemtablecreate(listpageurl,tabletype):
#根据list页面url创建一个数据表，如果tabletype=SP则创建一个包含商品sku编号以及商品名的表格，如果参数＝SR则创建一个ID－rate评价表
	dbconnect=MySQLdb.connect(host='localhost',user='root',passwd='hsj123',db='jd',charset='utf8')
	dbcurs=dbconnect.cursor()
	tablename=urlpageget(listpageurl,"div[@class='s-title']/h3[1]/b[1]","text()")[0]+tabletype
#	print type(tablename)
#	print chardet.detect(tablename)
#	print tablename
	if tabletype=="SP":
		try:	
	#	sql="drop table if exists %s;create table %s(skuid varchar(10),name text(40));"%(tablename,tablename)
	#	sql1="drop table if exists %s;"%tablename
	#	dbcurs.execute(sql1)
	#	dbconnect.commit()
			sql1="create table %s(skuid varchar(30),itemname text(60));"%tablename
			dbcurs.execute(sql1)
			dbconnect.commit()
			print tablename,"Skuid-Productname表格已创建成功!"
		except:
			print tablename,"Skuid-Productname表格创建失败!"
		dbcurs.close()
		dbconnect.close()
	if tabletype=="SR":
		try:
			sql2="create table %s(skuid varchar(30),5starrate int,4starrate int,3starrate int,2starrate int,1starrate int,totalcomment int);"%tablename
			dbcurs.execute(sql2)
			dbconnect.commit()
			print tablename,"Skuid-Rate表格已创建成功！"
		except:
			print tablename,"Skuid-Rate表格创建失败！"
		dbcurs.close()
		dbconnect.commit()

def urlpageget(url,tag="a",value="@href"):
#根据给定的url抓取页面内制定的标签链接
	if re.match(r'http://',url):
		try:
			pagereq=urllib2.Request(url,headers=header)
			time.sleep(random.random())
			pageres=urllib2.urlopen(pagereq).read()
			if pageres:
				pass
			else:
				print "页面为空！"
			try:
				encode=chardet.detect(pageres)['encoding']
				if encode=='gb2312':
					encode='gbk'
				pagehtml=etree.HTML(pageres.decode(encode,'ignore'))
				pageresponse=list()
				for i in pagehtml.xpath("//"+tag):
	#		pageresponse.update({i.xpath("text()")[0]:i.xpath("@href")[0]})
					if i.xpath(value):
						pageresponse.append(i.xpath(value)[0])
	#		print pageresponse
	#		print "标签信息获取成功！"
				return pageresponse
			except:
				print "标签信息获取失败！"
		except:
			print "链接打不开！"
	else:
		print "非正常url！"
	

def getlistpageitem(url,tag1,tag2,value):
#为获取list页面的所有skuid,以及产品名.list页面的商品陈列，并不是每个图片都只对应一个skuid，
#例如http://list.jd.com/list.html?cat=670,671,672页面中第9行的一个产品位置就对应了4个skuid，为获得所有某一类标签下所有的skuid以及name信息，需要特殊处理
	pagereq=urllib2.Request(url,headers=header)
	pageres=urllib2.urlopen(pagereq).read()
	encode=chardet.detect(pageres)['encoding']
	pagehtml=etree.HTML(pageres.decode(encode,'ignore'))
	values=[]
	for i in pagehtml.xpath(tag1):
#		print pagehtml.xpath(tag1)
		for j in i.xpath(tag2):
#			print i.xpath(tag2)
			values.append(j.xpath(value)[0])
	return values


def listpagenumandurl(url):
#获取list结果页面中底部的页面数，以及各页的url组成规律
	listresult=urlpageget(url,tag="span[@class='p-num'][1]//a",value="text()")
#得到的结果类似为['1', '-2', '-1', '0', '1', '2', '3', '353', u'\u4e0b\u4e00\u9875']，需要
#找出最大的那个数字,需要注意的是虽然看起来是字符串，但是其类型并不是str，‘1’，‘2’等这些数字字符串类型
#为lxml.etree._ElementStringResult，而文字字符串类型为lxml.etree._ElementUnicodeResult，
#注意type判断类型时不需要打引号
	pages=[int(i) for i in listresult if type(i)==lxml.etree._ElementStringResult]
	maxpagenum=max(pages) #结果页面最大数字
#分析各个结果页面url规律，<a href="/list.html?cat=1318,12147,12148&page=1&go=0&JL=6_0_0" class="curr">1</a>
#找到上面a链接中除去page＝后面的1的其余字符串
	listurlbase=urlpageget(url,tag="span[@class='p-num'][1]//a[@class='curr']",value="@href")
	listurlbasestring=''.join(listurlbase)
#	print listurlbasestring
	forehalfurl='http://list.jd.com'+''.join(listurlbasestring[0:listurlbasestring.index('page=')+5])
	endhalfurl=''.join(listurlbasestring[listurlbasestring.index('page=')+6:])
#	print forehalfurl+str(maxpagenum)+endhalfurl
	return forehalfurl,maxpagenum,endhalfurl

#返回的结果分别为每个list结果页面中所有产品的详情页的url的前，中，后部分。
#例如 http://list.jd.com/list.html?cat=670%2C671%2C672&page=355&JL=6_0_0
#对应前半部分为http://list.jd.com/list.html?cat=670%2C671%2C672&page=
#中间部分对应页面序号，355
#后半部分&JL=6_0_0


def writetxt(filename,content):
#content为列表，将其写入到一个文本文件中，进行保存
	txtfile=open(r"urltxts/"+filename+".txt","wa")
	for i in content:
		txtfile.write(i+'\n')
	txtfile.close()

def printlist(listpara,length='l'):
#打印列表内容
	for i in listpara:
		print i
	if length=='l':
		print "列表长度为：",len(listpara)

'''
def listsum(mainlist,tobeaddedlist):
#两个url列表相加，把第二个参数中的不同元素加入到第一个参数中，例如listsum([1,2,3,4],[2,3,5,7,8])=[1,2,3,4,5,7,8]
	for i in tobeaddedlist:
		if i not in mainlist:
			mainlist.append(i)
		else:
			pass
	return mainlist
'''
	
def listpagegetitemskuidnames(listurl):
#根据结果页的一个url获得所有结果页面陈列的商品的skuid以及对应产品的名字
	itemskuids=[]
	itemnames=[]
	if urlpageget(listurl,"div[@class='gl-i-tab-content']","div[1]"):
#list页面的商品陈列，并不是每个图片都只对应一个skuid，例如http://list.jd.com/list.html?cat=670,671,672页面中第9行的一个产品位置就对应了4个skuid，需要特殊处理
		itemskuids+=getlistpageitem(listurl,"//div[@class='gl-i-tab-content']","div[@class='tab-content-item j-sku-item']","@data-sku")
#		print "1:\n"
#		printlist(itemskuids)
		itemnames+=getlistpageitem(listurl,"//div[@class='gl-i-tab-content']","div[@class='tab-content-item j-sku-item']","div[3]/a[1]/em[1]/text()")
#		print "2:\n"
#		printlist(itemnames)
	if urlpageget(listurl,"div[@class='gl-i-tab-content']","//div[@class='tab-content-item tab-cnt-i-selected j-sku-item']"):
		itemskuids+=getlistpageitem(listurl,"//div[@class='gl-i-tab-content']","div[@class='tab-content-item tab-cnt-i-selected j-sku-item']","@data-sku")
#		print "3:\n"
#		printlist(itemskuids)
		itemnames+=getlistpageitem(listurl,"//div[@class='gl-i-tab-content']","div[@class='tab-content-item tab-cnt-i-selected j-sku-item']","div[3]/a[1]/em[1]/text()")
#		itemurls+=urlpageget(listurl,"li[@class='gl-item']/div[1]/div[1]/a[1]","@href")
#		print "4:\n"
#		printlist(itemnames)
	itemskuids+=urlpageget(listurl,"li[@class='gl-item']","div[1]/@data-sku")
	#print "5:\n"
	#printlist(itemskuids)
#	itemnames+=urlpageget(listurl,"li[@class='gl-item']/div[1]/div[3]/a[1]/em[1]","text()")
	itemnames+=urlpageget(listurl,"li[@class='gl-item']/div[1]/div[@class='p-name']/a[1]/em[1]","text()")
	#print "6:\n"
	#printlist(itemnames)
	#print "itemskuids长度：",len(itemskuids)
	#print "不重复的id个数为：",len(set(itemskuids))
	#print "itemnames长度：",len(itemnames)
	itemskuidsstringtype=[]
	itemnamesstringtype=[]
	tablename=urlpageget(listurl,"div[@class='s-title']/h3[1]/b[1]","text()")[0]
	if len(itemskuids)!=len(itemnames):
		print tablename,"结果页面skuid列表与itemname列表长度不一致"
	else:
		print tablename,"结果页面skuid列表与itemname列表长度一致,开始抓取……"	
	length=min([len(itemskuids),len(itemnames)])
	for i in range(length):
		itemskuidsstringtype.append(itemskuids[i])
		itemnamesstringtype.append(itemnames[i])
	print tablename,"类别下skuid以及productname已获取一页……"
	return itemskuidsstringtype,itemnamesstringtype

def listpagegetiteminfo(listpageurls):
#保存list页面中的产品类别名（每个产品类别建立一张表），url中cat后面的参数（作为键值），每个产品对应的页面链接，并将其存入数据库；
		for i in listpageurls:
			try:
				try:
					forehalfurl,maxpagenum,endhalfurl=listpagenumandurl(i)
					print i,"链接下共有",maxpagenum,"个结果页"
				except:
					print "链接",i,"页面规律获取失败！"
				itemskuids=[]
				itemnames=[]
				try:
					tablename=urlpageget(i,"div[@class='s-title']/h3[1]/b[1]","text()")[0]+"SP"
					print "已获取到一个表名",tablename,"待判断是否已在数据库中……"
				except:
					print "获取",i,"的表名失败！"
				existedtables=getdbtablenames()
				if  tablename in existedtables and dbtablenullornot(tablename):
					print tablename,"表格已存在于数据库中,且其中已有数据，跳过，继续抓取其他类别……"
				else:
					if tablename in existedtables and not dbtablenullornot(tablename):
						print tablename,"表格存在于数据库中，但没有数据，待抓取插入……"
					else:
						print "数据库中没有",tablename,"表格，待创建……"
						itemtablecreate(forehalfurl+"1"+endhalfurl,"SP")
						itemtablecreate(forehalfurl+"1"+endhalfurl,"SR")
					for j in range(1,maxpagenum+1):
						url=forehalfurl+str(j)+endhalfurl
						itemskuids+=listpagegetitemskuidnames(url)[0]
						itemnames+=listpagegetitemskuidnames(url)[1]
					try:
						manyinsertsql(tablename,itemskuids,itemnames)						
						print tablename,"表格已插入完毕！"
					except Exception,e:
						print tablename,"表格插入失败！错误信息:",e
			except:
				print "链接为",i,"下的信息获取失败，继续获取下一个链接的信息！"
				continue
		print "所有产品类别信息已抓取保存完毕，请在MySQL终端查询相关数据……"


def listpagenumandurl(url):
#获取list结果页面中底部的页面数，以及各页的url组成规律
	listresult=urlpageget(url,tag="span[@class='p-num'][1]//a",value="text()")
#得到的结果类似为['1', '-2', '-1', '0', '1', '2', '3', '353', u'\u4e0b\u4e00\u9875']，需要
#找出最大的那个数字,需要注意的是虽然看起来是字符串，但是其类型并不是str，‘1’，‘2’等这些数字字符串类型
#为lxml.etree._ElementStringResult，而文字字符串类型为lxml.etree._ElementUnicodeResult，
#注意type判断类型时不需要打引号
	pages=[int(i) for i in listresult if type(i)==lxml.etree._ElementStringResult]
	maxpagenum=max(pages) #结果页面最大数字
#分析各个结果页面url规律，<a href="/list.html?cat=1318,12147,12148&page=1&go=0&JL=6_0_0" class="curr">1</a>
#找到上面a链接中除去page＝后面的1的其余字符串
	listurlbase=urlpageget(url,tag="span[@class='p-num'][1]//a[@class='curr']",value="@href")
	listurlbasestring=''.join(listurlbase)
#	print listurlbasestring
	forehalfurl='http://list.jd.com'+''.join(listurlbasestring[0:listurlbasestring.index('page=')+5])
	endhalfurl=''.join(listurlbasestring[listurlbasestring.index('page=')+6:])
#	print forehalfurl+str(maxpagenum)+endhalfurl
	print "链接为",url,"的页面规律已获取！"
	return forehalfurl,maxpagenum,endhalfurl
#listpagenumandurl(urls[0])



def manyinsertsql(tablename,alist,blist):
#批量信息插入数据库中某个表
	dbconnect=MySQLdb.connect(host='localhost',user='root',passwd='hsj123',db='JD',charset='utf8')					
	dbcurs=dbconnect.cursor()
	if len(alist)!=len(blist):
		print "两个列表长度不一致，无法转化成元组列表插入数据库！"
	else:
		args=[]
		for i in range(len(alist)):
			args.append((alist[i],blist[i]))
		try:
			sql="insert into "+tablename.encode('utf-8')+" values(%s,%s)"  #注意sql语句中插入变量后要保持sql语法中的空格！！！
#			print sql
			dbcurs.executemany(sql,args)
			print "正在插入id-name信息到",tablename,"表格……"	
			dbconnect.commit()
		except Exception,e:
			print "插入id-name信息到",tablename,"表格发生错误！",e
	dbcurs.close()
	dbconnect.close()



def main():
	start=time.clock()
	print "程序开始运行，预计耗时较长，请耐心等候……"
	firstclass=firstclasspro()
#	printlist(firstclass,'l')
	suburllist=secondclasspro(firstclass)
	targeturls=preprocess(suburllist[0],'l')[0]
#	targeturls=urllistdiedai(suburllistprocess[0],suburllistprocess[1])
#	print "***************"
	printlist(targeturls,'l')
	dbcreate()
	listpagegetiteminfo(targeturls)
	end=time.clock()
	print "程序总计运行时间：",end-start,"秒"
	print "当前时间：",time.strftime('%Y-%m-%d %I:%M:%S',time.localtime())


if __name__ == '__main__':
	header={'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36','Referer':'http://www.jd.com/','Accept':'image/webp,image/*,*/*;q=0.8','Accept-Language':'zh-CN,zh;q=0.8,en;q=0.6'}
	main()



