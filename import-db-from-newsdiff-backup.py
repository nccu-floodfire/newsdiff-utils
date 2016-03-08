#!/usr/bin/env python3
#encoding=utf-8
import sys
import json
import mysql.connector
import getopt
import ast

set_ids = set()

is_dryrun = False
action = ''

opts, args = getopt.getopt(sys.argv[1:], '', ["action=", 'file=', 'dryrun', 'insert-diff'])
for opt in opts:
    if opt[0] == '--action':
        action = opt[1]
    elif opt[0] == '--file':
        filename = opt[1]
    elif opt[0] == '--dryrun':
        is_dryrun = True
    elif opt[0] == '--insert-diff':
        is_insert_diff = True


if len(action) == 0:
    sys.stderr.write("--action is required\n");


if action == 'import':
    f = open(filename, 'r')

    counter = 0
    total = 0
    title = ""
    content = ""
    obj = None

    cnx = mysql.connector.connect(host='127.0.0.1', user='root', password='', database='newsdiffreport', charset='utf8')
    cursor = cnx.cursor()
    is_exists = False
    update_count = 0
    insert_count = 0

    for line in f:
        line = line.strip('\n')
        counter = counter+1

        if total % 1000 == 0 and counter % 3 == 0:
            print("count: %s, insert_count: %s, update_count: %s" % (total, insert_count, update_count))
        if counter == 1:
            obj = None
            obj = json.loads(line)


            id = obj['id']
            if id in set_ids:
                is_exists = True
            else:
                set_ids.add(id)
                is_exists = False
            continue

        if counter == 2:
            line = line.strip('"').replace('\\n', '\n').replace('\\r', '\r').replace('\\/', '/')
            title = line
            continue

        if counter == 3:
            total = total + 1

            line = line.strip('"').replace('\\n', '\n').replace('\\r', '\r')
            line = line.strip('"').replace('\\n', '\n').replace('\\r', '\r').replace('\\/', '/')
            content = line
            counter = 0

            if not is_exists:
                sql = "insert into news(id, url, normalized_id, normalized_crc32, source, created_at, last_fetch_at, last_changed_at, error_count) values(%s, %s, %s, %s, %s, %s, %s, %s, %s);"
                data = (obj['id'], obj['url'], obj['normalized_id'], obj['normalized_crc32'], obj['source'], obj['created_at'], obj['last_fetch_at'], obj['last_changed_at'], obj['error_count'])

                if is_dryrun:
                    print(sql)
                    print(data)
                else:
                    cursor.execute(sql, data)

                    sql = "insert into news_info(news_id, time, title, body) values(%s, %s, %s, %s);"
                    data = (obj['id'], obj['version'], title, content)
                    cursor.execute(sql, data)
                    insert_count = insert_count + 1
                    cnx.commit()
            else:
                if is_insert_diff:
                    if is_dryrun:
                        print('id exists, insert diff')
                    else:
                        sql = "insert into news_info(news_id, time, title, body) values(%s, %s, %s, %s);"
                        data = (obj['id'], obj['version'], title, content)
                        cursor.execute(sql, data)
                        insert_count = insert_count + 1
                        cnx.commit()

    print("Finished: " + str(total))
    f.close()
    cursor.close()
    cnx.close()

