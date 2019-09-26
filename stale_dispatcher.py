#!/opt/rh/rh-python36/root/usr/bin/python3
#A. Tolkachev, 89326188877

'''
Программа выполняет подачу файлов с вызовами из директории stale на тарификацию\n')
Загрузка файлов выполняется в  три этапа:
Шаг 1. Фасуем файлы в папки по датам вызовов с сохранением иерархии
       После подачи файлов в stale в одной папке содержатся файлы с вызовами за различные даты.
       Для выполнения критериев по загрузке вызовов, нам необходима предварительная оценка количества вызовов до 7 дней и старше
       Файлы, содержащие в себе вызовы за определённую дату, перемещаются в папку с соответсвующей датой в директорию store
       При этом внутри папки, куда был перемещен файл, создаётся поддиректория указывающая на направление вызова
       Пример:
          Пусть файл содержит в себе локальные онлайн вызовы за 1 января 2019 года. Если ещё не была создана, будет создана папка
          /ocs_loc/BRT/stale/store/2019-01-01/lon, куда и  будет перемещен наш файл
print('Шаг 2. Выполняется сбор статистики
       После того как будут перемещены все файлы из директории stale в директорию store, будет произведен расчёт количества вызовов
       в разрезе направлений как отдельно для каждой из папок в директории store, так и для всех вызовов в директории store')
       результат будет записан в файл counter.log
       Пример:
          Информация по всем вызовам в директории store будет содержаться в файле /ocs_loc/BRT/stale/store/counter.log
          Локальных онлайн вызовов:    664381
          Локальных оффлайн вызовов:   1921
          Роуминговых онлайн вызовов:  10
          Роуминговых оффлайн вызовов: 0
          Вызовов на вставку не младше семи дней: 99980
          Вызовов на вставку старше семи дней: 566332
          Всего файлов: 666312
Шаг 3. Выполняется подача файлов на тарификацию согласно переданному при запуске скрипта режиму
       Тестовый режим - 0
           Выполняются 1 и 2 шаг. Данный режим используется для сбора статистики. Эта информация необходима для оценки масштабов загрузки, а также для проведения
           информирования перед проведением загрузки (запуском скрипта в других режимах) и, в случае не соблюдения критериев загрузки - для получения согласования.
       Режим соблюдения критериев - 1
           Выполняются 1 и 2 шаг. На 3-ем шаге файлы из директорий с датами не старше 7 дней подаются на тарификацию в том
           случае если их количество не превышает 10 000 000 вызовов. Файлы из директорий
           старше 7 дней подаются на тарификацию в случае если их количество не превышает 1 000 000. Информация о загрузке помещается в файлы в папку logs
       Подача на тарификацию всех файлов - 2
           Выполняются 1 и 2 шаг. На тарификацию подаются абсолютно все файлы из директории store. Информация о загрузке помещается в файлы в папку logs.
           Данный режим можно использовать только в случае получения согласования от ФРИМАН. При этом необходимо произвести информирование коллег из nexign-systems.
           Перед запуском программы с данным режимом  необходимо разместить информацию на сайте https://megawiki.megafon.ru/pages/viewpage.action?pageId=151917832
Пример запуска скрипта: ./stale_dispatcher.py 2
По вопросам работы программы обращаться к Толкачеву Александру
'''

import time
import os
import string
import re
import socket
import sys
import shutil
from datetime import datetime, timedelta
import cx_Oracle
import subprocess
from subprocess import check_output


#Объявление ключевых переменных
portion = 500 # Порция файлов
time_break = 60 #Пауза в секундах после подачи порции файлов. Также используется в случае если переполнена входная директория

if len(sys.argv)==1:
   mode = 0
elif len(sys.argv)>=3:
   mode = 0   
elif sys.argv[1]:
   if sys.argv[1] == 'help':
      print(__doc__)
      sys.exit()   
   elif int(sys.argv[1]) >=0 and int(sys.argv[1]) <=2:
      mode = sys.argv[1]      
   else:
      mode = 0      

print('Используется режим - '+str(mode)+'. Для ознакомления с режимами наберите ./stale_dispatcher help\n')

#Находим минимальную дату в файле, возвращаем количество строчек в нём
def GetMinDateInFile(file_name):
    file = open(file_name,"r")
    line_counter=0
    for line in file:
       if line.strip()=='':
         exit
       strlist = line.split(',')
       if line_counter==0:
          min_data=strlist[7]
          line_counter+=1
          continue 
       if (int(strlist[7])< int(min_data)):
          min_data=int(strlist[7])
       line_counter+=1
    file.close()
    min_data = str(min_data)
    finaly_data=min_data[0:4]+'-'+min_data[4:6]+'-'+min_data[6:8]
    return finaly_data
    
#Рекурсивный поиск файлов в директории
def GetFileListInDirectory(input_path):
    file_list = []
    for file in os.listdir(input_path):
       path = os.path.join(input_path, file)
       if not os.path.isdir(path):
          file_list.append(path)
       else:
          file_list += GetFileListInDirectory(path)
    return file_list
# filePath

#Создание всех необходимых директорий и перемещение файлов
def MoveFiles(file_path,file_date):
    destination= os.path.abspath(os.curdir) #pwd
    arch_path = file_path.split('/')
    if not os.path.exists(os.path.join(destination,'store')):
       os.mkdir(os.path.join(destination,'store'))
    destination = os.path.join(destination,'store')
    if not os.path.exists(os.path.join(destination,file_date)):
       os.mkdir(os.path.join(destination,file_date)) 
    destination = os.path.join(destination,file_date)
    if not os.path.exists(os.path.join(destination,arch_path[5])):
       os.mkdir(os.path.join(destination,arch_path[5]))
    destination = os.path.join(destination,arch_path[5])
    #os.makedirs(destination+'/store/'+file_date+'/'+arch_path[5])
    destination = os.path.join(destination, os.path.basename(file_path))
    #print('Пермещаю файл  из директории '+ file_path+' в папку '+destination)
    os.rename(file_path, destination)

#Проверка количества файлов в директории
def CheckDirectory (directory,limit):
   number_of_files = len(next(os.walk(directory))[2])
   if  number_of_files < limit:
      return True
   else:
      return False            

#Удаление пустых директорий 
def RemoveEmptyDirectory(directory, regexp, exception):
   for source_dir in sorted(os.listdir(directory)):
      if re.fullmatch(regexp,source_dir):
         file_list = GetFileListInDirectory (os.path.join(directory,source_dir))
         for i in file_list:
            if exception in i:
               file_list.remove(i)
         if len(file_list)==0:
            print('   Удалена пустая директория '+os.path.join(directory,source_dir))
            shutil.rmtree(os.path.join(directory,source_dir))


#Подача файлов на тарификацию
def FileLoader (load_directory,portion,pause):
   counter = 0
   partition_name ='CALLS_'+re.sub('-','',os.path.basename(load_directory))
   destination = dict()
   destination['lon'] = '/ocs_loc/BRT/OFFLINE/IN_'
   destination['lof'] = '/ocs_loc/BRT/RTDSC/LOCAL/IN_'
   destination['ron'] = '/ocs_loc/BRT/ROAM/BIS/IN_'
   destination['rof'] = '/ocs_loc/BRT/RTDSC/ROAM/IN_'
            
   source=os.path.join(os.path.abspath(os.curdir),'store')
   d_source=os.path.join(source,load_directory)
   
   db = cx_Oracle.connect('BIS', 'ReUAb_YnBV0W_Vn', 'GFBIS_UTAG')
   cursor = db.cursor()
   
   #Вставляем в базу информацию о том в какие дни велась загрузка в виде партиций, для дальнейшего ускорения сбора статистики
   cursor.execute ('insert into alexander_a_tolkache.list_of_partitions values (\''+partition_name+'\',\''+datetime.now().strftime("%d.%m.%Y")+'\')')
   db.commit()
   
   if os.path.exists(d_source):
      d_list = os.listdir(d_source)
      if 'counter.log' in d_list:
         d_list.remove('counter.log')
      for d_file in d_list:
         if d_file in destination:
            f_source = os.path.join(d_source,d_file)
            path_to = destination[d_file]
            file_on_load = GetFileListInDirectory(f_source)
            print('   Выполняется загрузка из директории '+os.path.join(d_source, d_file))
            for z in file_on_load:
               rows = []
               #Вставка данных по вызову в базу
               file = open(z,"r")
               for line in file:
                  #rows = []
                  word = line.split(',')
                  row = (word[0],word[5],word[7])
                  rows.append(row)
                  file.close
               cursor.prepare ('insert into alexander_a_tolkache.belated_insert_calls (select :1,:2, to_date(:3,\'yyyymmddhh24miss\'), sysdate,null, null from dual)')
               cursor.executemany(None, rows)      
               db.commit()
                     
               outlog_directory ='./logs/stale_download_'+datetime.now().strftime("%Y%m%d")+'.log'
               logFile = open(outlog_directory,"a")
               if counter%2 == 0:
                  tail = 'M0'
               else:
                  tail = 'S0'
               #Провереям количество файлов в директории назначения, если она перегружена меняем  M0 на SO (либо наоборот). Если перегружены обе, то программа уходит в sleep
               while CheckDirectory(path_to + tail, 500) == False:
                  if tail == 'M0':
                     tail = 'S0'
                  else:
                     tail = 'M0'
                  if CheckDirectory(path_to + 'S0', 500) == False and CheckDirectory(path_to + 'M0', 800) == False:                                           
                     print('   Обе директории ' +path_to +'M0(S0) переполнены, работа программы прервана на '+ str(pause) +' секунд')
                     details.write('   Обе директории ' +path_to +'M0(S0) переполнены, работа программы прервана на '+ str(pause) +' секунд')
                     time.sleep(pause)
               file_names = os.path.basename(z)                  
               if os.path.basename(z).count('_stale') == 0 and os.path.basename(z).count('.stale') == 0:
                  file_names = os.path.basename(z)+'_stale'      
               finaly_path_to = os.path.join(path_to + tail,file_names)
               while CheckDirectory('/ocs_loc/CHARGESDB/CHARGING_DATA_CONVEYER/CALLS/in', 150) == False:
                  details.write('   Директория /ocs_loc/CHARGESDB/CHARGING_DATA_CONVEYER/CALLS/in переполнена, работа программы прервана на '+ str(pause) +' секунд')
                  print('   Директория /ocs_loc/CHARGESDB/CHARGING_DATA_CONVEYER/CALLS/in переполнена, работа программы прервана на '+ str(pause) +' секунд')
                  time.sleep(pause) 
               os.rename(z,finaly_path_to)
               logFile.write(datetime.now().strftime("%d-%m-%Y %H:%M")+' перемещение '+z+' -> '+finaly_path_to+'\n')
               logFile.close()
               counter+=1
               if counter%portion==0:
                  details.write('\n   Плановая пауза после подачи '+str(portion)+' файлов')
                  print('   Плановая пауза после подачи '+str(portion)+' файлов')
                  time.sleep(pause)
      print('      Загрузка из директории '+load_directory+' завершена в '+datetime.now().strftime("%d-%m-%Y %H:%M")+'. Всего было подано файлов на тарификацию - '+str(counter))
      details.write('\n      Загрузка из директории '+load_directory+' завершена в '+datetime.now().strftime("%d-%m-%Y %H:%M")+'. Всего файлов было подано на тарификацию - '+str(counter))
      db.close()

#--------------------------------------------------------------------
#MAIN BLOCK
print('Начало работы программы '+datetime.now().strftime("%d-%m-%Y %H:%M"))
if not os.path.exists('./store'):
    os.mkdir('./store')

if not os.path.exists('./logs'):
    os.mkdir('./logs')
    
details = open('./logs/details.log',"a")
details.write('\n----------------------------------------')    
details.write('\nВремя запуска программы '+datetime.now().strftime("%d-%m-%Y %H:%M"))

source = os.path.abspath(os.curdir)
text_look_for = r"20\d{2}-\d{2}-\d{2}"

print('Шаг 1. Фасую файлы в папки по датам вызовов с сохранением иерархии')
details.write('\nШаг 1. Фасую файлы в папки по датам вызовов с сохранением иерархии')
for s_file in os.listdir(source):
    if re.fullmatch(text_look_for,s_file):	
          directory = os.path.join(source,s_file)
       #Расскомментить чтобы исключить какую либо папку из загрузки, папки можно перечислить через запятую
       #if directory not in ('/ocs_loc/BRT/stale/2019-06-03'): 
          new_list = GetFileListInDirectory (directory)
          #print(new_list)
          if len(new_list)==0:
             shutil.rmtree(directory)
          #print('Удалена пустая папка '+directory)
          else:                               
             d_file = os.listdir(directory)
             for i in d_file:
                if (i == 'lon' or i == 'lof' or i == 'ron' or i == 'rof'):
                   if os.path.exists(os.path.join(directory,i)):
                      load_directory = os.path.join(directory,i)
                      print('   Происходит перемещение файлов из директории '+load_directory+' в папку store')
                      details.write('\n   Происходит перемещение файлов из директории '+load_directory+' в папку store')
                      file_list = GetFileListInDirectory (load_directory)
                      for j in file_list:
                         try: 
                            if os.stat(j).st_size == 0:
                               os.remove(j)
                               print('      Удалён файл'+j+'. Причина: Файл пуст')
                               details.write('\n      Удалён файл'+j+'. Причина: Файл пуст')
                               continue                      
                            min_data = GetMinDateInFile(j)
                            if int(min_data[0:4]+min_data[5:7]) < int(datetime.now().strftime("%Y")+datetime.now().strftime("%m"))-1:             
                               os.remove(j)
                               print('      Удалён файл'+j+'. Причина: Файл содержит вызовы ранее 01-'+str('%02d' % (datetime.now().month-1))+'-'+str(datetime.now().year))
                               details.write('\n      Удалён файл'+j+'. Причина: Файл содержит вызовы ранее 01-'+str('%02d' % (datetime.now().month-1))+'-'+str(datetime.now().year))
                               continue
                            MoveFiles(j,min_data)
                         except PermissionError:
                            details.write('\n      Ошибка: Нет доступа на перемещение файла '+j)
                            print('      Ошибка: Нет доступа на перемещение файла '+j)                            
                else: 
                   continue                   
details.write('\n   Шаг 1 - завершен. Файлы расфасованы. Время: '+ datetime.now().strftime("%d-%m-%Y %H:%M"))
print('   Шаг 1 - завершен. Файлы расфасованы. Время: '+ datetime.now().strftime("%d-%m-%Y %H:%M"))

#Блок сбора статистики о очереди не поданных на тарификацию вызовов
#------------------------------------------------------------------
i=0
authorized_date=[]
while (i<7):
   dateBorder = str(datetime.today() - timedelta(i))
   dateBorder = dateBorder[0:10]
   authorized_date.append(dateBorder)
   i+=1

if int(mode) != 2:
   print('Шаг 2. Выполняется сбор статистики')
   details.write('\nШаг 2. Выполняется сбор статистики')
   destination=os.path.join(os.path.abspath(os.curdir),'store')

   #Для общей статистики
   main_direction = dict()
   main_direction['lon']=0
   main_direction['lof']=0
   main_direction['ron']=0
   main_direction['rof']=0

   days7 = 0
   rest = 0
   for file in os.listdir(destination):
#Для статистики отдельно по каждой из папок
     direction = dict()
     direction['lon']=0
     direction['lof']=0
     direction['ron']=0
     direction['rof']=0
     if re.fullmatch(text_look_for,file):
        cdr_file_list = GetFileListInDirectory(os.path.join(destination,file))
        if len(cdr_file_list)==0:
           shutil.rmtree(os.path.join(destination,file)) 
           #print('Удалена пустая папка '+os.path.join(destination,file))
           continue
        for i in cdr_file_list:
           arch = i.split('/')
           if arch[6]=='counter.log':
              continue
           counter = sum(1 for line in open(i, 'r'))
           if arch[6] in direction:
              direction[arch[6]]+=counter
              main_direction[arch[6]]+=counter
           if file in authorized_date:
              days7+=counter
           else:
              rest+=counter                       
        fl = open(os.path.join(destination,file,'counter.log'),"w+")       
        fl.write('Локальных онлайн вызовов:    '+str(direction['lon'])+'\n')
        fl.write('Локальных оффлайн вызовов:   '+str(direction['lof'])+'\n')
        fl.write('Роуминговых онлайн вызовов:  '+str(direction['ron'])+'\n')
        fl.write('Роуминговых оффлайн вызовов: '+str(direction['rof'])+'\n') 
        fl.close()
   fmain = open(os.path.join(destination,'counter.log'),"w+")  	    
   fmain.write('Локальных онлайн вызовов:    '+str(main_direction['lon'])+'\n')
   fmain.write('Локальных оффлайн вызовов:   '+str(main_direction['lof'])+'\n')
   fmain.write('Роуминговых онлайн вызовов:  '+str(main_direction['ron'])+'\n')
   fmain.write('Роуминговых оффлайн вызовов: '+str(main_direction['rof'])+'\n')
   fmain.write('Вызовов на вставку младше семи дней: '+str(days7)+'\n')
   fmain.write('Вызовов на вставку старше семи дней: '+str(rest)+'\n')
   fmain.write('Всего вызовов: '+str(days7+rest))
   fmain.close()
   details.write('\n   Шаг 2 - завершен. Статистика собрана. Время: '+ datetime.now().strftime("%d-%m-%Y %H:%M"))
   print('   Шаг 2 - завершен. Статистика собрана. Время: '+ datetime.now().strftime("%d-%m-%Y %H:%M"))
#Блок загрузки информации в базу 
source = os.path.join(os.path.abspath(os.curdir),'store')


if int(mode) == 1 or int(mode) == 2:
   print('Шаг 3. Выполняется подача файлов на тарификацию согласно переданному режиму mode = '+str(mode))
   source = os.path.join(os.path.abspath(os.curdir),'store')
   download_counter=0
   
# Создаём pid файл
   pidFile = open('stale_dispatcher.pid', 'w+')
   pidFile.write(str(os.getpid()))
   pidFile.close()
     
   
if  int(mode)==2:
   print('   Подаю все вызовы из директории store на тарификацию')
         
   for source_dir in sorted(os.listdir(source)):
      load_dir = os.path.join(source, source_dir)
      if source_dir != 'counter.log':
         FileLoader(load_dir,portion,time_break) 
   print('   Шаг 3 - завершен. Вызовы вставлены в базу.')          
   details.write('\n   Шаг 3 - завершен. Вызовы вставлены в базу.')
                       
if int(mode) == 1:
   if days7 < 10**7 and days7 > 0:
      print('   Подаю на тарификацию вызовы не старше 7 дней')
      details.write('\n   Подаю на тарификацию вызовы не старше 7 дней')
      for i in sorted(authorized_date):
         FileLoader(i,portion,time_break)
   if rest < 10**6 and rest > 0: 
      print('   Подаю на тарификацию вызовы старше  7 дней') 
      details.write('\n   Подаю на тарификацию вызовы старше  7 дней')
      for source_dir in sorted(os.listdir(source)):
         if re.fullmatch(text_look_for,source_dir) and source_dir not in authorized_date and source_dir != 'counter.log':
            load_dir = os.path.join(source, source_dir)
            FileLoader(load_dir,portion,time_break)
   details.write('\n   Шаг 3 - завершен. Вызовы вставлены в базу. Время: '+ datetime.now().strftime("%d-%m-%Y %H:%M"))
   print('   Шаг 3 - завершен. Вызовы вставлены в базу.')            
#Удаляем пусты папки

RemoveEmptyDirectory(source,text_look_for,'counter.log')#store
source = os.path.abspath(os.curdir)
RemoveEmptyDirectory(source,text_look_for,'cache')#stale


#print('Удаляем пустые директории из store')
#for source_dir in os.listdir(source):
#   new_list = GetFileListInDirectory (os.path.join(source,source_dir))
#   print(new_list)
#   if len(new_list) == 1:
#      shutil.rmtree(os.path.join(source,source_dir))
#      print('  Удалена пустая папка '+os.path.join(source,source_dir))

if os.path.isfile('stale_dispatcher.pid'): 
   #print('   Удаляем pid файл')     
   os.remove('stale_dispatcher.pid')

details.write('\nПрограмма завершила свою работу '+datetime.now().strftime("%d-%m-%Y %H:%M")+'\n')   
details.close()                           
print('Программа закончила свою работу '+datetime.now().strftime("%d-%m-%Y %H:%M"))