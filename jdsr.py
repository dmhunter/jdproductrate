#-*- coding: UTF-8 -*-
import sys
import os
import lxml
import xmllib
reload(sys)
sys.setdefaultencoding('utf-8')
from lxml import etree
from lxml import html
import re
import urllib2
import chardet
import json
import random
import time
import MySQLdb
def getdbskuid(tablename,columnname="skuid"):
#从数据库中取出某个表格的所有skuid
	dbconnect=MySQLdb.connect(host='localhost',user='root',passwd='hsj123',db='JD',charset='utf8')
	curs=dbconnect.cursor()
	sql="select %s from %s;"%(columnname,tablename)
	curs.execute(sql)
	dbconnect.commit()
	ids=[]
	while True:
		result=curs.fetchone()
		if result:
		  	skuid=result[0].encode('utf-8')
		  	ids.append(skuid)
		else:
			break
	return ids


def srtablecreate():
#创建已有sptable的对应srtable
	dbconnect=MySQLdb.connect(host='localhost',user='root',passwd='hsj123',db='JD',charset='utf8')
	dbcurs=dbconnect.cursor()
	if set(getdbtablenames("SP"))==set(getdbtablenames("SR")):
		print "SP与SR表格已配对，暂不需要新建表格……"
	else:
		for i in getdbtablenames("SP"):
			srtablename=i[:-2]+"SR"
			if srtablename not in getdbtablenames("SR"):
				print srtablename,"不在数据库中，待创建……"				
				sql="create table %s(skuid varchar(30),5starrate int,4starrate int,3starrate int,2starrate int,1starrate int,totalcomment int);"%srtablename
				try:
					dbcurs.execute(sql)
					dbconnect.commit()
					print srtablename,"创建成功！"
				except:
					print srtablename,"创建失败！"
			else:
				print srtablename,"已存在于SR表中……"				
				continue
	print "当前SR已全部创建完成！"


def spsrtableidcheck(sptablename):
#检查某个sptable中skuid是否都存在于对应的srtable中，如果不存在则返回对应的skuid列表。如果sp,sr表格skuid一致，则不做抓取处理
	if set(getdbskuid(sptablename))==set(getdbskuid(sptablename[:-2]+"SR")):
		print sptablename[:-2],"SR评价已完整存在于数据库中，暂不抓取……"
	else:
		tobeaddedids=list(set(getdbskuid(sptablename))-set(getdbskuid(sptablename[:-2]+"SR")))
		ratelist=[]
		for i in tobeaddedids:
			try: 
				ratelist.append(getcommentjsonfile(i))
				print sptablename[:-2],"SR表中skuid为",i,"的商品评价已抓取……"
			except:
				print sptablename[:-2],"SR表中skuid为",i,"的商品评价抓取失败，继续抓取下一个skuid评价……"
				continue
		srratelist=delnoneturple(ratelist)
		return ratelist

def delnoneturple(ratelist):
#调试程序期间发现ratelist中有None类型元组，这里删除掉ratelist中所有的非法元组
	while None in ratelist:
		ratelist.remove(None)
	return ratelist


def getdbtablenames(tabletype):
#获取数据库中已经存在的SP表或SR表，返回表名列表
	dbconnect=MySQLdb.connect(host='localhost',user='root',passwd='hsj123',db='JD',charset='utf8')
	dbcurs=dbconnect.cursor()
	dbcurs.execute("show tables;")
	dbconnect.commit()
	result=dbcurs.fetchall()
	if result:
		existedtables=[]
		if tabletype=="SP":
			for i in result:
				if i[0][-2:]=="SP":
					existedtables.append(i[0])
			return existedtables
		if tabletype=="SR":
 			for i in result:
				if i[0][-2:]=="SR":
					existedtables.append(i[0])
			return existedtables
	else:
		print "数据库中没有表格！"
		return []
#注：result结果是一个元组列表
	dbcurs.close()
	dbconnect.close()



def manyinsertratetable(tablename,ratelist):
#将评价信息批量写入sr表
	dbconnect=MySQLdb.connect(host='localhost',user='root',passwd='hsj123',db='JD',charset='utf8')
	curs=dbconnect.cursor()
#	print tablename,"字符编码为:",chardet.detect(tablename),"\n"
	sql="insert into "+tablename.encode('utf-8')+" values(%s,%s,%s,%s,%s,%s,%s)"
	try:
		curs.executemany(sql,ratelist)
		dbconnect.commit()
		print "正在插入评价信息到",tablename,"表格……"
	except Exception,e:
		print "插入评价信息到",tablename,"失败!错误为：",e
	curs.close()
	dbconnect.close()


def rateexistornot(rate):
#判断抓取到的rate是否已经存在于数据库中，如果数据库中没有，则返回true，否则返回false
	allskuid=[]
	for i in getdbtablenames:
		for j in getdbskuid(i)[1]:
			allskuid.append(j)
	if rate[0] in allskuid:
		print rate[0],"评价信息已存在于数据库中,暂不存入……"
		return False
	else:
		return True


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


def itemtablecreate(listpageurl,tabletype):
#根据list页面url创建一个数据表，如果tabletype=SP则创建一个包含商品sku编号以及商品名的表格，如果参数＝SR则创建一个ID－rate评价表
	dbconnect=MySQLdb.connect(host='localhost',user='root',passwd='hsj123',db='jd',charset='utf8')
	dbcurs=dbconnect.cursor()
	tablenamedecode=urlpageget(listpageurl,"div[@class='s-title']/h3[1]/b[1]","text()")[0]+tabletype
	tablename=tablenamedecode.encode('utf-8')
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
		dbconnect.close()


def getcommentjsonfile(id):
#根据获得的商品的id获取评论页的json文件
#	for i in itemskuidsstringtype:
	commentpagenum=0
#	commentjson=[]
	commentjsonurl="http://club.jd.com/productpage/p-"+id+"-s-3-t-3-p-"+str(commentpagenum)+".html?callback=fetchJSON_comment98vv36846"
	#commentjsonurl="http://club.jd.com/productpage/p-1510479-s-3-t-3-p-0.html?callback=fetchJSON_comment98vv36846"
#	errortimes=0
#	commentjsonurl="http://club.jd.com/productpage/p-"+id+"-s-3-t-3-p-"+str(commentpagenum)+".html?callback=fetchJSON_comment98vv"+str(round(random.random()*10000+30000))
	try:
		pagereq=urllib2.Request(commentjsonurl,headers=header)
		#print "+++++++++++++++"
		pageres=urllib2.urlopen(pagereq).read()
		#print "###############"
		pagecode=chardet.detect(pageres)["encoding"]
		pagestring=pageres.decode(pagecode).encode("utf-8")
		#print "***************"
			#print type(pagestring)
			#print jsonstring(pagestring)
			#print "@@@@@@@@@@@@@@@" 
			#code=chardet.detect(jsonstring(pageres))["encoding"]
			#print code
		commentjson=json.loads(jsonstring(pagestring))
		#print "================"
		#print commentjson
		#print type(commentjson)
		#print "@@@@@@@@@@@@@@@@"
		try:
			rate=(id,commentjson['productCommentSummary']['score5Count'],commentjson['productCommentSummary']['score4Count'],commentjson['productCommentSummary']['score3Count'],commentjson['productCommentSummary']['score2Count'],commentjson['productCommentSummary']['score1Count'],commentjson['productCommentSummary']['commentCount'])
			print rate
			return rate
		except Exception,e:
			print e
		#print "###############"
#		commentpagenum+=1
	except:
		print "skuid为",id,"的商品评价信息抓取失败！"
#			errortimes+=1
#			if errortimes<3:
#				print "第",commentpagenum+1,"页评论获取失败！继续抓取……"
#				pass
#			else:
#				print "第",commentpagenum+1,"页评论获取失败！抓取终止！"
#				break		
	

def jsonstring(htmlstring):
#获取的评论页的json字符串，截去除开最外层的{}之外的字符，并将{}中的所有换行以及空格删除
	string1=htmlstring[htmlstring.index("(")+1:-2]
	#string2=string1.replace(" ","")
	string3=string1.replace("\n","")
	return string3

def insertrate():
#将抓取的评价信息存入数据库
	srtablecreate()
	for i in getdbtablenames("SP"):
		try:
			manyinsertratetable(i[:-2]+"SR",spsrtableidcheck(i))
			print i[:-2],"SR表格评价信息已写入完毕！"
			time.sleep(2)
		except Exception,e:
			print i[:-2],"SR表格评价信息写入错误，继续下一个表格……"
			print "错误说明：",e
			continue
	print "所有表格评价信息已写入完毕，请在mysql终端查看！"


def main():
	start=time.clock()
	insertrate()
	end=time.clock()
	print "程序总计运行时间：",end-start,"秒"
	print "当前时间：",time.strftime('%Y-%m-%d %I:%M:%S',time.localtime())
if __name__ == "__main__":
	header={"Accept":"*/*","Accept-Encoding":"gzip, deflate, sdch","Accept-Language":"zh-CN,zh;q=0.8,en;q=0.6","Connection":"keep-alive","Cookie":"_tp=4N0eybF20YXlGbBq%2B%2FWPtw%3D%3D; _pst=jingdongker; TrackID=14YuhLFDJztEI3ptrWrDoHwUrAW6C6IJPUyeHQXQFzN7MA-dfYvBT8LqVtK-TCeqwfCM9QJojzpOTNoGVtpjFjgI3fm6vBSfWT1STUQJoDONCSRZmEc2qLhCeNX0PMSB3; pinId=lEOWdda3K21nMrHebMdBLw; unick=jingdongker; user-key=e1942588-2349-42b6-b7aa-b0b787ad5392; cn=0; e_png=f200754bd908422faa97e0eb3f0e64c3908381638; e_etag=f200754bd908422faa97e0eb3f0e64c3908381638; 3AB9D23F7A4B3C9B=f200754bd908422faa97e0eb3f0e64c3908381638; __jda=122270672.295511575.1451577344.1452595109.1452601249.30; __jdc=122270672; __jdv=122270672|direct|-|none|-; ipLocation=%u5317%u4EAC; areaId=1; ipLoc-djd=1-72-2799-0; __jdu=295511575",'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36',"Host":"club.jd.com",'Referer':'http://www.jd.com/','Accept':'image/webp,image/*,*/*;q=0.8','Accept-Language':'zh-CN,zh;q=0.8,en;q=0.6',"Referer":"http://item.jd.com/1510479.html"}
	main()



