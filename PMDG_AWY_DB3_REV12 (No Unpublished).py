import pandas as pd
import os
import time
import sqlite3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def dms_to_decimal_latitude(dms):
    direction = dms[0]
    degrees = int(dms[1:3])
    minutes = int(dms[3:5])
    seconds = int(dms[5:])
    
    decimal = degrees + (minutes / 60) + (seconds / 3600)
    
    if direction == 'S':
        decimal = -decimal
    
    return round(decimal, 8)

def dms_to_decimal_longitude(dms):
    direction = dms[0]
    degrees = int(dms[1:4])
    minutes = int(dms[4:6])
    seconds = int(dms[6:])
    
    decimal = degrees + (minutes / 60) + (seconds / 3600)
    
    if direction == 'W':
        decimal = -decimal
    
    return round(decimal, 8)

def km_to_nm(km):
    return round(km * 0.539957, 2)

def match_icao_code(earth_fix_file, earth_nav_file, waypoint_identifier, code_type):
    start_time = time.time()

    icao_codes = {"ZB", "ZS", "ZJ", "ZG", "ZY", "ZL", "ZU", "ZW", "ZP", "ZH"}

    try:
        if code_type == "DESIGNATED_POINT":
            with open(earth_fix_file, 'r', encoding='utf-8') as file:
                lines = file.readlines()
                for line in lines:
                    parts = line.split()
                    if len(parts) >= 5 and parts[2] == waypoint_identifier and parts[4] in icao_codes and parts[3] == "ENRT":
                        return parts[4]

        elif code_type in {"VORDME", "NDB"}:
            with open(earth_nav_file, 'r', encoding='utf-8') as file:
                lines = file.readlines()
                for line in lines:
                    parts = line.split()
                    if len(parts) >= 10 and parts[7] == waypoint_identifier and parts[9] in icao_codes and parts[8] == "ENRT":
                        if (parts[-1] == "NDB" and code_type == "NDB") or (parts[-1] == "VOR/DME" and code_type == "VORDME"):
                            return parts[9]
    except FileNotFoundError as e:
        logging.error(f"File not found: {e}")
        return None

    logging.info(f"未找到 {waypoint_identifier} 的区域代码，搜索完成。")
    logging.info(f"搜索耗时: {time.time() - start_time:.2f} 秒")
    return None

def check_route_exists(cursor, route_identifier, waypoint_identifier):
    query = """
    SELECT COUNT(*) FROM tbl_enroute_airways
    WHERE route_identifier = ? AND waypoint_identifier = ?
    """
    cursor.execute(query, (route_identifier, waypoint_identifier))
    count = cursor.fetchone()[0]
    return count > 0  # 如果数量大于0，表示存在相同记录

def csv_to_db(csv_file, db_file, earth_fix_file, earth_nav_file, encoding='utf-8'):
    logging.info(f"尝试使用编码 {encoding} 读取文件: {csv_file}")

    try:
        if not os.path.isfile(csv_file):
            raise FileNotFoundError(f"文件 '{csv_file}' 不存在。")
        
        df = pd.read_csv(csv_file, encoding=encoding, on_bad_lines='warn')
        logging.info("文件读取成功。")
        
    except FileNotFoundError as fnf_error:
        logging.error(fnf_error)
        return
    except Exception as e:
        logging.error(f"读取CSV文件失败: {e}")
        return

    # 连接SQLite数据库
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # 确保表格存在
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tbl_enroute_airways (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            route_identifier TEXT,
            area_code TEXT DEFAULT 'EEU',
            icao_code TEXT,
            waypoint_identifier TEXT,
            end_waypoint_identifier TEXT,
            waypoint_latitude REAL,
            waypoint_longitude REAL,
            waypoint_description_code TEXT,
            outbound_course REAL,
            inbound_distance REAL,
            seqno INTEGER,
            id TEXT,
            route_type TEXT DEFAULT 'R',
            flightlevel TEXT DEFAULT 'B',
            crusing_table_identifier TEXT DEFAULT 'EE',
            minimum_altitude1 INTEGER DEFAULT 0,
            minimum_altitude2 INTEGER DEFAULT NULL,
            maximum_altitude INTEGER DEFAULT 99999
        )
    ''')

    route_last_row = {}
    log_entries = []

    # 初始化变量
    current_route_identifier = None
    route_counter = 1
    previous_outbound_course = 0
    route_last_row = {}
    
    for _, row in df.iterrows():
        try:
            waypoint_latitude = dms_to_decimal_latitude(row['GEO_LAT_START_ACCURACY'])
            waypoint_longitude = dms_to_decimal_longitude(row['GEO_LONG_START_ACCURACY'])
            inbound_distance = km_to_nm(float(row['VAL_LEN']))
            waypoint_identifier = row['CODE_POINT_START']
            end_waypoint_identifier = row['CODE_POINT_END']
            code_type = row['CODE_TYPE_START']
            end_code_type = row['CODE_TYPE_END']
            route_identifier = row['TXT_DESIG']
            outbound_course = row['VAL_MAG_TRACK']
    
            # 搜索icao_code
            icao_code = match_icao_code(earth_fix_file, earth_nav_file, waypoint_identifier, code_type)
    
            if code_type == "DESIGNATED_POINT":
                waypoint_description_code = 'E C'
                id = f'tbl_enroute_waypoints|{icao_code}{waypoint_identifier}'
            elif code_type == "VORDME":
                waypoint_description_code = 'V C'
                id = f'tbl_vhfnavaids|{icao_code}{waypoint_identifier}'
            elif code_type == "NDB":
                waypoint_description_code = 'E C'
                id = f'tbl_enroute_ndbnavaids|{icao_code}{waypoint_identifier}'
            else:
                waypoint_description_code = ''
                id = ''
    
            # 检查是否已存在相同的记录
            if check_route_exists(cursor, route_identifier, waypoint_identifier):
                print(f"航路 {route_identifier} 的航点 {waypoint_identifier} 已存在，跳过该航路。")
                continue  # 跳过这段航路
            
            if route_identifier != current_route_identifier:
                current_route_identifier = route_identifier
                
                # 获取要覆盖的航路的最小seqno
                cursor.execute('''
                    SELECT MIN(seqno) FROM tbl_enroute_airways 
                    WHERE route_identifier = ? AND waypoint_identifier = ?
                ''', (route_identifier, waypoint_identifier))
                min_seqno = cursor.fetchone()[0]
            
                if min_seqno is None:
                    # 如果航路不存在，将min_seqno设置为1000
                    min_seqno = 1000
            
                # Reset route counter for new route
                route_counter = 1
                
                # Reset the previous outbound course for the new route
                previous_outbound_course = 0
            
            # 计算新的seqno
            new_seqno = min_seqno + (route_counter * 10)
            route_counter += 1
            
            # Determine inbound_course
            inbound_course = previous_outbound_course
    
            # Insert new route data
            cursor.execute('''
                INSERT INTO tbl_enroute_airways 
                (route_identifier, area_code, icao_code, waypoint_identifier, waypoint_latitude, waypoint_longitude, waypoint_description_code, outbound_course, inbound_course, inbound_distance, seqno, id, route_type, flightlevel, crusing_table_identifier, minimum_altitude1, minimum_altitude2, maximum_altitude)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (route_identifier, 'EEU', icao_code, waypoint_identifier, waypoint_latitude, waypoint_longitude, waypoint_description_code, outbound_course, inbound_course, inbound_distance, new_seqno, id, 'R', 'B', 'EE', 0, None, 99999))
            
            # Record previous outbound course for the next waypoint
            previous_outbound_course = outbound_course
            
            # 记录每个route_identifier的最后一行
            route_last_row[route_identifier] = (new_seqno, waypoint_description_code, end_waypoint_identifier, end_code_type)
    
        except KeyError as ke:
            logging.error(f"缺少的列：{ke}")
    
    # 更新每段航路的最后一个航点并添加终点
    for route_identifier, (last_seqno, description_code, waypoint_identifier, code_type) in route_last_row.items():
        try:
            # 搜索终点的icao_code
            logging.info(f"查找终点 {waypoint_identifier} 的 ICAO 代码，航路 {route_identifier}，类型 {code_type}")
            icao_code = match_icao_code(earth_fix_file, earth_nav_file, waypoint_identifier, code_type)
            logging.info(f"查找结果: {icao_code}")
    
            if code_type == "DESIGNATED_POINT":
                waypoint_description_code = 'EEC' if description_code == 'E C' else description_code
                id = f'tbl_enroute_waypoints|{icao_code}{waypoint_identifier}'
            elif code_type == "VORDME":
                waypoint_description_code = 'VEC' if description_code == 'V C' else description_code
                id = f'tbl_vhfnavaids|{icao_code}{waypoint_identifier}'
            elif code_type == "NDB":
                waypoint_description_code = 'EEC' if description_code == 'E C' else description_code
                id = f'tbl_enroute_ndbnavaids|{icao_code}{waypoint_identifier}'
            else:
                waypoint_description_code = ''
                id = ''

            # 查找上一点的 outbound_course
            filtered_df = df[(df['TXT_DESIG'] == route_identifier) & (df['CODE_POINT_END'] == waypoint_identifier)]
            if not filtered_df.empty:
                inbound_course = int(filtered_df['VAL_MAG_TRACK'].values[0])
            else:
                inbound_course = 0

            # 使用终点航点的经纬度信息
            waypoint_df = df[(df['CODE_POINT_END'] == waypoint_identifier)]
            if not waypoint_df.empty:
                geo_lat_accuracy = waypoint_df['GEO_LAT_END_ACCURACY'].values[0]
                geo_long_accuracy = waypoint_df['GEO_LONG_END_ACCURACY'].values[0]
                waypoint_latitude = dms_to_decimal_latitude(geo_lat_accuracy)
                waypoint_longitude = dms_to_decimal_longitude(geo_long_accuracy)
            new_seqno = last_seqno + 10
            outbound_course = 0.0
            inbound_distance = 0.0

            # 插入新终点航点数据
            cursor.execute('''
                INSERT INTO tbl_enroute_airways 
                (route_identifier, area_code, icao_code, waypoint_identifier, waypoint_latitude, waypoint_longitude, waypoint_description_code, outbound_course, inbound_course, inbound_distance, seqno, id, route_type, flightlevel, crusing_table_identifier, minimum_altitude1, minimum_altitude2, maximum_altitude)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (route_identifier, 'EEU', icao_code, waypoint_identifier, waypoint_latitude, waypoint_longitude, waypoint_description_code, outbound_course, inbound_course, inbound_distance, new_seqno, id, 'R', 'B', 'EE', 0, None, 99999))
    
        except KeyError as ke:
            logging.error(f"缺少的列：{ke}")
    

    # 提交更改
    conn.commit()

    # 查询没有 ICAO 代码的记录
    cursor.execute('''
        SELECT waypoint_identifier, waypoint_latitude, waypoint_longitude
        FROM tbl_enroute_airways 
        WHERE icao_code IS NULL AND waypoint_description_code != ''
    ''')
    
    rows = cursor.fetchall()
    
    for row in rows:
        waypoint_identifier, waypoint_latitude, waypoint_longitude = row
        
        # 匹配纬度和经度
        cursor.execute('''
            SELECT icao_code
            FROM tbl_enroute_airways
            WHERE waypoint_latitude = ? AND waypoint_longitude = ?
              AND icao_code IN ("ZB", "ZS", "ZJ", "ZG", "ZY", "ZL", "ZU", "ZW", "ZP", "ZH")
        ''', (waypoint_latitude, waypoint_longitude))
        
        matching_row = cursor.fetchone()
        if matching_row:
            new_icao_code = matching_row[0]
            logging.info(f'找到纬度 {waypoint_latitude} 和经度 {waypoint_longitude} 的 ICAO 代码: {new_icao_code}')
        else:
            new_icao_code = None
            logging.info(f'未找到纬度 {waypoint_latitude} 和经度 {waypoint_longitude} 的 ICAO 代码')
        
        # 检查是否需要设置 ICAO 代码为 "VH"
        if not new_icao_code:
            if 20 <= waypoint_latitude <= 23 and 112 <= waypoint_longitude <= 115 or waypoint_identifier == 'MAGOG':
                new_icao_code = "VH"
                logging.info(f'纬度 {waypoint_latitude} 和经度 {waypoint_longitude} 在指定范围内。设置 ICAO 代码为 "VH"')
            elif 31.8 <= waypoint_latitude <= 38.75 and 124.75 <= waypoint_longitude <= 131:
                new_icao_code = "RK"
                logging.info(f'纬度 {waypoint_latitude} 和经度 {waypoint_longitude} 在指定范围内。设置 ICAO 代码为 "RK"')
            elif waypoint_identifier == 'TX558':
                new_icao_code = "ZY"
                logging.info(f'纬度 {waypoint_latitude} 和经度 {waypoint_longitude} 在指定范围内。设置 ICAO 代码为 "ZY"')
            elif waypoint_identifier == 'ZDQ':
                new_icao_code = "ZW"
                logging.info(f'纬度 {waypoint_latitude} 和经度 {waypoint_longitude} 在指定范围内。设置 ICAO 代码为 "ZW"')
            else:
                logging.info(f'纬度 {waypoint_latitude} 和经度 {waypoint_longitude} 不在指定范围内。不设置 ICAO 代码')
        
        # 更新记录的 ICAO 代码
        cursor.execute('''
            UPDATE tbl_enroute_airways 
            SET icao_code = ? 
            WHERE waypoint_identifier = ?
        ''', (new_icao_code, waypoint_identifier))
    
    # 提交更改并关闭数据库连接
    conn.commit()
    conn.close()

    # 输出日志信息
    for entry in log_entries:
        logging.info(entry)

    logging.info("CSV数据已成功导入并处理。")

# 示例用法
csv_file = "/Users/lujuncheng/Downloads/XP导航数据/RTE_SEG.csv"
db_file = "/Users/lujuncheng/Downloads/MSFS/pmdg-aircraft-77w-v2-0-33.rar/pmdg-aircraft-77w/Config/NavData/e_dfd_PMDG.s3db"
earth_fix_file = "/Users/lujuncheng/Library/Application Support/Steam/steamapps/common/X-Plane 12/Custom Data/earth_fix.dat"
earth_nav_file = "/Users/lujuncheng/Library/Application Support/Steam/steamapps/common/X-Plane 12/Custom Data/earth_nav.dat"

csv_to_db(csv_file, db_file, earth_fix_file, earth_nav_file, encoding='utf-8')

