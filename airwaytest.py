# 从 earth_nav.dat 文件中获取经纬度
def get_earth_nav_coordinates(waypoint_identifier, earth_nav_file):
    try:
        with open(earth_nav_file, 'r', encoding='utf-8') as file:
            for line in file:
                # 使用 split 处理可能的多空格
                parts = line.split()

                # 跳过无效行
                if len(parts) < 10:
                    continue

                # 提取 .dat 文件的字段
                current_waypoint = parts[8]
                latitude = parts[1]
                longitude = parts[2]
                waypoint_type = parts[-1]  # 假设最后一个字段是 VOR/DME 或 NDB
                icao_code = parts[9]  # 假设第10个字段是 ICAO code

                # 检查是否满足条件
                if current_waypoint == waypoint_identifier and \
                   waypoint_type in ['VOR/DME', 'NDB'] and \
                   icao_code in ['ZW', 'ZG', 'ZS', 'ZY', 'ZL', 'ZH', 'ZU', 'ZP', 'ZB', 'ZJ']:
                    return latitude, longitude  # 返回找到的经纬度
    except FileNotFoundError:
        print(f"文件 {earth_nav_file} 未找到")
    except Exception as e:
        print(f"处理文件 {earth_nav_file} 时出现错误: {e}")

    return None  # 如果未找到，返回 None

# 主函数来处理 VORDME 或 NDB 类型的航路点
def process_vor_or_ndb_coordinates(waypoint_identifier, waypoint_type, db_file, earth_nav_file):
    # Step 1: 从 tbl_vhfnavaids 中尝试查找
    coordinates = get_vhfnav_coordinates(waypoint_identifier, db_file)

    if coordinates:
        return coordinates  # 如果找到，直接返回
    else:
        # Step 2: 从 earth_nav.dat 文件中查找
        coordinates = get_earth_nav_coordinates(waypoint_identifier, earth_nav_file)

    return coordinates if coordinates else (None, None)  # 返回经纬度或 None

# 主逻辑修改 - 将其整合进现有代码
def process_waypoint(waypoint_identifier, code_id, db_file, earth_nav_file):
    # 检查航路点类型是否为 VORDME 或 NDB
    if code_id in ['VORDME', 'NDB']:
        # 使用已有的经纬度数据而不是计算
        latitude, longitude = process_vor_or_ndb_coordinates(
            waypoint_identifier, code_id, db_file, earth_nav_file
        )

        if latitude and longitude:
            print(f"使用已有数据: {waypoint_identifier} 的经纬度为: {latitude}, {longitude}")
        else:
            print(f"未找到 {waypoint_identifier} 的现有经纬度")
    else:
        # 对于非 VORDME 或 NDB 的航路点，执行其他逻辑
        print(f"处理其他航路点: {waypoint_identifier}")
