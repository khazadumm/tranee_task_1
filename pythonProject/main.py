# This is a sample Python script.
import json
import xml.etree.ElementTree as ET
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import psycopg2
def load_files(students  = 'C:\\Users\\user\\Downloads\\students.json',\
               rooms = 'C:\\Users\\user\\Downloads\\rooms.json' ):
    print('s' , students, rooms)
    with open(students, 'r') as target_student:
        file_content_student = target_student.read()
        students_list = json.loads(file_content_student)
    sql_insert = "INSERT INTO hostel.students(birthday,id,name,room,sex) VALUES "
    sql_insert += ','.join(f"('{i.get('birthday')}',{i.get('id')}, '{i.get('name')}',{i.get('room')},'{i.get('sex')}')" for i in students_list) + ';'
    print(sql_insert)
    insert_sql(sql_insert, 'students')
    with open(rooms, 'r') as target_rooms:
        file_content_rooms = target_rooms.read()
        rooms_list = json.loads(file_content_rooms)
    sql_insert = "INSERT INTO hostel.rooms(id,name) VALUES "
    sql_insert += ','.join(f"({i.get('id')}, '{i.get('name')}')" for i in rooms_list) + ';'
    print(sql_insert)
    insert_sql(sql_insert,'rooms')
def insert_sql(sql, table):
    try:
        connection = psycopg2.connect(
            dbname="postgres",
            user="habrpguser",
            password="pgpwd4habr",
            host="localhost",
            port="5432"
        )
        cursor = connection.cursor()
        cursor.execute('DELETE FROM hostel.' + table + ' WHERE 1=1;')
        connection.commit()
        cursor.execute(sql)
        connection.commit()
        print('Success')
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        connection.close()

def write_json(data, report_num):
    with open('data.json', 'w') as f:
        match report_num:
            case 1:
                json.dump([{'Room': i[0], 'Count_Students': i[1]} for i in data], f)
            case 2:
                json.dump([{'Room': i[0], 'AVG_Age': i[1]} for i in data], f)
            case 3:
                json.dump([{'Room': i[0], 'Diff_Age': i[1]} for i in data], f)
            case 4:
                json.dump([{'Room': i[0]} for i in data], f)

def write_xml(data, report_num = 1):
    report = ET.Element("report")
    report = ET.SubElement(report, "report")
    match report_num:
        case 1:
            for i in data:
                room = ET.SubElement(report, "room")
                room.text = i[0]
                count_students = ET.SubElement(report, "count_students")
                count_students.text = str(i[1])
        case 2:
            for i in data:
                room = ET.SubElement(report, "room")
                room.text = i[0]
                avg_age = ET.SubElement(report, "avg_age")
                avg_age.text = str(i[1])
        case 3:
            for i in data:
                room = ET.SubElement(report, "room")
                room.text = i[0]
                diff_age = ET.SubElement(report, "diff_age")
                diff_age.text = str(i[1])
        case 4:
            for i in data:
                room = ET.SubElement(report, "room")
                room.text = i[0]

    tree = ET.ElementTree(report)
    tree.write("data.xml", encoding='utf-8', xml_declaration=True)

def return_reports(rep_num: int = 1 , format = 'json'):
    if rep_num not in (1,2,3,4):
        print('Wrong number of report')
        return None
    """
        rep_name in:
        1 - Список комнат и количество студентов в каждой из них

        2 - 5 комнат, где самый маленький средний возраст студентов

        3 - 5 комнат с самой большой разницей в возрасте студентов

        4 - Список комнат где живут разнополые студенты
    """
    try:
        connection = psycopg2.connect(
            dbname="postgres",
            user="habrpguser",
            password="pgpwd4habr",
            host="localhost",
            port="5432"
        )
        cursor = connection.cursor()
        match rep_num:
            case 1:
                sql = "select r.name as room,count(s.id) as count_of_students "\
                    "from hostel.rooms r inner join hostel.students s on (r.id = s.room) "\
                    "group by r.name order by 1"
            case 2:
                sql = "select r.name as room, "\
                        "avg( DATE_PART('year', now()) - DATE_PART('year', date(substring(s.birthday, 1, 10)))) "\
                        "as avg_age from hostel.rooms r inner join hostel.students s on (r.id = s.room) "\
                        "group by 1 order by 2 limit 5"
            case 3:
                sql = "select r.name as room,"\
                        "max( DATE_PART('year', now()) - DATE_PART('year', date(substring(s.birthday, 1, 10)))) "\
                        "- min( DATE_PART('year', now()) - DATE_PART('year', date(substring(s.birthday, 1, 10)))) as diff_age"\
                        " from	hostel.rooms r inner join hostel.students s on	(r.id = s.room)"\
                        " group by 1 order by 2 desc limit 5"
            case 4:
                sql = "select room from ("\
                        "select r.name as room,count(distinct s.sex) as cnt_sex "\
                        "from hostel.rooms r inner join hostel.students s on	(r.id = s.room)"\
                        "group by 1 having count(distinct s.sex)>1) a"

        cursor.execute(sql)
        if format == 'json':
            write_json(cursor.fetchall(),rep_num)
        else:
            write_xml(cursor.fetchall(),rep_num)
        for i in cursor.fetchall():
            print(i)
        print('Success')
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        connection.close()
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    while True:
        print('Выберите опцию:')
        print('1 - загрузка таблиц')
        print('2 - выгрузка отчетов')
        print('3 - выход')
        inp = int(input())
        match inp:
            case 1:
                print('Укажите путь к файлу students')
                st_in = input()
                print('Укажите путь к файлу rooms')
                r_in = input()
                if st_in == '' and r_in != '' :
                    load_files(rooms=r_in)
                elif r_in == '' and st_in != '' :
                    load_files(students=st_in)
                elif r_in == '' and st_in == '' :
                    load_files()
                else:
                    load_files(st_in,r_in)
            case 2:
                print('Укажите номер отчета')
                n_in = input()
                print('Укажите выходной формат')
                out_in = input()
                return_reports(int(n_in), out_in)
            case 3:
                break
        if inp not in (1,2,3):
            print('Неверный пункт меню')


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
