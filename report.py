import parser
import numpy as np
import pandas as pd
import sys
from mysql.connector import MySQLConnection


def connectDB():
    """Подключение к БД"""
    try:
        confConnectBD = parser.read_config(section='connectBD')
        connect = MySQLConnection(**confConnectBD)
        cursor = connect.cursor()
        return connect, cursor

    except Exception as e:
        print(e)

def select_data(query):
    """Выполнить sql select"""
    try:
        connect, cursor = connectDB()
        cursor.execute(query)
        data = cursor.fetchall()

        return data

    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connect.close()

def statistic(startDate, endDate):
    """Получение статистики"""

    # Запрос для подсчета статистики по результатам участий в соревнованиях
    query = """SELECT concat(stat_pupil.lastname,' ',stat_pupil.firstname), stat_pupil.sex, stat_pupil.birthday, stat_pupil.teacher, stat_event.name, stat_event.date, 
                      stat_type.type, stat_result.rang
                FROM stat_pupil 
                LEFT JOIN stat_result ON stat_result.pupilid = stat_pupil.id 
                LEFT JOIN stat_event ON stat_result.eventid = stat_event.id
                LEFT JOIN stat_type ON stat_event.typeid = stat_type.id
                WHERE rang in (1,2,3,'у','вне')
                AND stat_event.date BETWEEN '%s' AND '%s'""" % (startDate, endDate)
    dataResult = pd.DataFrame(select_data(query), columns=['name','sex','age','teacher','event','date','type','rang'])

    # Запрос по учащимся
    query = """SELECT concat(stat_pupil.lastname,' ',stat_pupil.firstname),stat_pupil.teacher, stat_pupil.sex, stat_pupil.birthday
                FROM stat_pupil where status = 0 or (status = 1 and date_remove >='%s' and date_remove<= '%s') """ % (startDate, endDate)
    dataPupil = pd.DataFrame(select_data(query), columns=['name', 'teacher', 'sex', 'age'])

    # Запрос по соревнованиям
    query = """SELECT name, date, status, type FROM `stat_event`
               JOIN stat_type ON stat_type.id = stat_event.typeid
               AND stat_event.date BETWEEN '%s' AND '%s'""" % (startDate, endDate)
    dataEvent = pd.DataFrame(select_data(query), columns=['name', 'date', 'status', 'type'])
    
    #запрос по выборке нулевых участников который я хз как сделать в пандасе 
    query = """SELECT teacher, count(*) FROM stat_pupil 
                WHERE not exists (
                SELECT 1 FROM stat_result 
                JOIN stat_event ON stat_result.eventid = stat_event.id
                WHERE pupilid = stat_pupil.id AND stat_event.date BETWEEN '%s' AND '%s') 
                GROUP BY teacher 
                ORDER BY count(*) DESC""" % (startDate, endDate)
    
    dataNullPupil = pd.DataFrame(select_data(query), columns=['teacher', 'count'])
    

    dataPupil['age'] = pd.to_datetime(dataPupil['age'])
    
    
    styles = [{'selector':"*", 'props':[
                    ("font-family" , 'Mono'),
                    ("font-size" , '15px'),
                    ("border" , "1px solid #ccc"),
                ]},
                {'selector':"caption", 'props':[
                    ("background-color", "cyan"),
                    ("text-align", "center"),
                    ("font-weight", "bold"),
                    ("color", "#000"),
                ]},
                {'selector':"th", 'props':[
                    ("padding" , "0 10px"),
                ]},
                {'selector':"th.index_name", 'props':[
                    ("background-color", "#f7ff39"),
                ]}
                
    ]

    df_sex_count = dataPupil.sex.value_counts()
    df_sex_count = df_sex_count.rename_axis("Пол")
    df_sex_count.name = "Количество"
    
    df_sex_per = dataPupil.sex.value_counts()/dataPupil.sex.count()
    df_sex_per = df_sex_per.rename_axis("Пол")
    df_sex_per.name = "Проценты"
    
    df_sex =  pd.concat([df_sex_count, df_sex_per], axis= 1 )
    
    df_age_count = dataPupil.groupby(dataPupil.age.dt.year)['age'].count()
    df_age_count = df_age_count.rename_axis("Год")
    df_age_count.name = "Количество"
    
    df_age_per = dataPupil.groupby(dataPupil.age.dt.year)['age'].count() / dataPupil.age.count()
    df_age_per = df_age_per.rename_axis("Год")
    df_age_per.name = "Проценты"
    
    df_age =  pd.concat([df_age_count, df_age_per], axis= 1)
    
    df_event_count = dataResult.event.count()
    df_pupil_event_count = dataResult.name.drop_duplicates().count()
    df_viner = dataResult[dataResult.rang.isin(['1','2','3'])]['name'].drop_duplicates().count()
    df_vinner_all = dataResult[dataResult.rang.isin(['1', '2', '3'])]['name'].count()
    df_event_type = dataEvent.name.drop_duplicates().count()
    df_evebt_view =  dataEvent[dataEvent.status != '']['status'].count()
    
    df_stat = pd.DataFrame(np.vstack([df_event_count, df_pupil_event_count,df_viner,df_vinner_all,df_event_type, df_evebt_view]), 
                index=['Всего участий по спискам', 'Человек участвовало в мероприятиях по спискам','Призёров (хотя бы один раз)','Всего раз становились призёрами обучающиеся','Мероприятия по положениям','Мероприятия по видам'], 
                columns=['Количество'])
                
    df_even_partic = dataEvent[['name', 'type']].drop_duplicates(subset=['name'])['type'].value_counts()
    df_even_partic = df_even_partic.rename_axis("Вид")
    df_even_partic.name = "Количество"
    
    df_rang = dataResult[dataResult.rang.isin(['1','2','3'])]['type'].value_counts()
    df_rang = df_rang.rename_axis("Тип")
    df_rang.name = "Количество"
    
    df_vinner_rang = dataResult[dataResult.rang.isin(['1', '2', '3'])][['rang','name']].drop_duplicates()['rang'].value_counts().sort_values()
    df_vinner_rang = df_vinner_rang.rename_axis("Места")
    df_vinner_rang.name = "Количество"
    
    df_rang_count = dataResult[dataResult.rang.isin(['1', '2', '3'])].groupby(['type', 'rang'])['type'].value_counts()
    df_rang_count = df_rang_count.rename_axis(["Вид","Места"])
    df_rang_count.name = "Количество"
    
    df_rang_event = dataResult[dataResult.rang.isin(['1', '2', '3'])].groupby(['date','event', 'rang'])['event'].value_counts()
    df_rang_event = df_rang_event.rename_axis(["Дата","Соревнования","Места"])
    df_rang_event.name = "Количество призеров"
    
    df_rang_teacher = dataResult[dataResult.rang.isin(['1', '2', '3'])].groupby(['teacher', 'rang'])['teacher'].value_counts()
    df_rang_teacher = df_rang_teacher.rename_axis(["Тренер","Места"])
    df_rang_teacher.name = "Количество призеров"
    
    df_teacher_count = dataResult['teacher'].value_counts()
    df_teacher_count = df_teacher_count.rename_axis(["Тренер"])
    df_teacher_count.name = "Количество участий"
    
    dataNullPupil.rename(columns={'teacher':'Тренер', 'count':'Количство'}, inplace=True)
    
    print(df_sex.style.set_caption('Пол').format({'Проценты':"{:.2%}"}).set_table_styles(styles).to_html(), end='</br>')
    print(df_age.style.set_caption('Возраст').format({"Проценты":"{:.2%}"}).set_table_styles(styles).to_html(), end='</br>')
    print(df_stat.style.set_caption('Всякое разное').set_table_styles(styles).to_html(), end='</br>')
    print(df_even_partic.to_frame().style.set_caption('Количество мероприятий, в которых участвовали').set_table_styles(styles).to_html(), end='</br>')
    print(df_rang.to_frame().style.set_caption('Занимали призовые места').set_table_styles(styles).to_html(), end='</br>')
    print(df_vinner_rang.to_frame().style.set_caption('Количество призеров по местам').set_table_styles(styles).to_html(), end='</br>')
    print(df_rang_count.to_frame().style.set_caption('Количество призеров по типам соревнований').set_table_styles(styles).to_html(), end='</br>')
    print(df_rang_event.to_frame().style.set_caption('Количество призеров по соревнованиям').set_table_styles(styles).to_html(), end='</br>')
    print(df_rang_teacher.to_frame().style.set_caption('Количество призеров по тренерам').set_table_styles(styles).to_html(), end='</br>')
    print(df_teacher_count.to_frame().style.set_caption('Количество участий по тренерам').set_table_styles(styles).to_html(), end='</br>')
    print(dataNullPupil.style.set_caption('Количество нулевых участий по тренерам').set_table_styles(styles).to_html(), end='</br>')


if __name__ == '__main__':

    statistic(sys.argv[1], sys.argv[2])



