import psycopg2  # Thư viện để kết nối và thao tác với cơ sở dữ liệu PostgreSQL
from psycopg2 import sql  # Module hỗ trợ tạo câu lệnh SQL an toàn
import os  # Thư viện để kiểm tra sự tồn tại của file

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Hàm tải dữ liệu từ file ratings vào bảng ratings trong cơ sở dữ liệu PostgreSQL.
    - Mục đích: Tạo bảng ratings và nhập dữ liệu từ file test_data.dat (định dạng UserID::MovieID::Rating::Timestamp).
    - Tác dụng: Chuẩn bị dữ liệu gốc cho các hàm phân mảnh (rangepartition, roundrobinpartition) và chèn dữ liệu (rangeinsert, roundrobininsert).
    - Schema bảng ratings: userid (INTEGER), movieid (INTEGER), rating (FLOAT).
    - Quy trình: Xóa bảng cũ nếu tồn tại, tạo bảng tạm với cột bổ sung để xử lý dấu ::, nhập dữ liệu từ file, xóa cột không cần thiết.

    Tham số:
    - ratingstablename (str): Tên bảng ratings, thường là 'ratings'.
    - ratingsfilepath (str): Đường dẫn tuyệt đối đến file dữ liệu (test_data.dat).
    - openconnection: Đối tượng kết nối đến cơ sở dữ liệu PostgreSQL (từ testHelper.getopenconnection).

    Biến chính:
    - con: Đối tượng kết nối đến cơ sở dữ liệu.
    - cur: Con trỏ (cursor) để thực thi các lệnh SQL.
    """
    # Kiểm tra file dữ liệu có tồn tại không
    # ratingsfilepath: Đường dẫn đến file test_data.dat (ví dụ: /path/to/test_data.dat)
    if not os.path.exists(ratingsfilepath):
        return  # Thoát hàm nếu file không tồn tại
    
    # con: Gán đối tượng kết nối từ tham số openconnection
    # cur: Tạo con trỏ để thực thi lệnh SQL
    con = openconnection
    cur = con.cursor()
    
    # Xóa bảng ratings nếu đã tồn tại để tránh xung đột
    # ratingstablename: Tên bảng sẽ được xóa và tạo mới
    cur.execute(f"DROP TABLE IF EXISTS {ratingstablename}")
    
    # Tạo bảng tạm thời với các cột bổ sung để xử lý định dạng file (UserID::MovieID::Rating::Timestamp)
    # Cột tạm: extra1, extra2, extra3 lưu dấu ::, timestamp lưu giá trị không cần thiết
    cur.execute(
        f"CREATE TABLE {ratingstablename} (userid INTEGER, extra1 CHAR, movieid INTEGER, extra2 CHAR, rating FLOAT, extra3 CHAR, timestamp BIGINT);")
    
    # Nhập dữ liệu từ file vào bảng bằng phương thức copy_from
    # Mở file ratingsfilepath, dùng dấu phân tách ':' để tách các cột
    cur.copy_from(open(ratingsfilepath), ratingstablename, sep=':')
    
    # Xóa các cột tạm để bảng ratings có schema đúng (userid, movieid, rating)
    cur.execute(
        f"ALTER TABLE {ratingstablename} DROP COLUMN extra1, DROP COLUMN extra2, DROP COLUMN extra3, DROP COLUMN timestamp;")
    
    cur.close()
    con.commit()
    

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm phân mảnh bảng ratings theo phương pháp Range Partitioning dựa trên cột rating (khoảng giá trị từ 0 đến 5).
    - Mục đích: Chia dữ liệu trong bảng ratings thành N bảng phân mảnh (range_part0, range_part1, ..., range_part{N-1}),
                mỗi bảng chứa các bản ghi có rating nằm trong một khoảng xác định.
    - Tác dụng: Tối ưu hóa truy vấn theo khoảng giá trị rating (ví dụ: tìm tất cả bản ghi có rating từ 3 đến 4).
    - Quy trình: Chia khoảng rating (0-5) thành N phần đều, mỗi phần có độ rộng delta = 5/N.
                Tạo bảng phân mảnh và chèn dữ liệu từ ratings vào bảng tương ứng dựa trên rating.

    Tham số:
    - ratingstablename (str): Tên bảng ratings, thường là 'ratings'.
    - numberofpartitions (int): Số lượng phân mảnh cần tạo (N, ví dụ: 5).
    - openconnection: Đối tượng kết nối đến cơ sở dữ liệu PostgreSQL.

    Biến chính:
    - con: Đối tượng kết nối đến cơ sở dữ liệu.
    - cur: Con trỏ để thực thi lệnh SQL.
    - delta (float): Độ rộng của mỗi khoảng rating (5.0 / numberofpartitions).
    - RANGE_TABLE_PREFIX (str): Tiền tố tên bảng phân mảnh, mặc định là 'range_part'.
    - minRange (float): Giới hạn dưới của khoảng rating cho mỗi phân mảnh.
    - maxRange (float): Giới hạn trên của khoảng rating cho mỗi phân mảnh.
    - table_name (str): Tên bảng phân mảnh (range_part0, range_part1, ...).
    """
    # Kiểm tra số phân mảnh có hợp lệ không
    # numberofpartitions: Số lượng bảng phân mảnh cần tạo
    if numberofpartitions <= 0:
        return  # Thoát hàm nếu N <= 0 (không tạo phân mảnh)
    
    # con: Gán đối tượng kết nối
    # cur: Tạo con trỏ SQL
    con = openconnection
    cur = con.cursor()
    
    # Tính độ rộng của mỗi khoảng rating
    # delta: Khoảng cách giữa các khoảng rating, đảm bảo chia đều từ 0 đến 5
    delta = 5.0 / numberofpartitions
    
    # RANGE_TABLE_PREFIX: Tiền tố để đặt tên bảng phân mảnh
    RANGE_TABLE_PREFIX = 'range_part'
    
    # Lặp qua N phân mảnh để tạo bảng và chèn dữ liệu
    for i in range(numberofpartitions):
        # Tính giới hạn của khoảng rating cho phân mảnh i
        minRange = i * delta  # Giới hạn dưới (ví dụ: 0, 1, 2, ...)
        maxRange = minRange + delta  # Giới hạn trên (ví dụ: 1, 2, 3, ...)
        table_name = RANGE_TABLE_PREFIX + str(i)  # Tên bảng: range_part0, range_part1, ...
        
        # Tạo bảng phân mảnh với schema giống bảng ratings
        cur.execute(f"CREATE TABLE {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT);")
        
        # Chèn dữ liệu từ ratings vào bảng phân mảnh
        # Với i=0: Bao gồm cả giá trị minRange (rating >= minRange)
        # Với i>0: Không bao gồm minRange (rating > minRange) để tránh trùng lặp
        if i == 0:
            cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) SELECT userid, movieid, rating FROM {ratingstablename} WHERE rating >= {minRange} AND rating <= {maxRange};")
        else:
            cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) SELECT userid, movieid, rating FROM {ratingstablename} WHERE rating > {minRange} AND rating <= {maxRange};")
    
    cur.close()
    con.commit()
    

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm phân mảnh bảng ratings theo phương pháp Round-Robin Partitioning.
    - Mục đích: Chia dữ liệu trong bảng ratings thành N bảng phân mảnh (rrobin_part0, rrobin_part1, ..., rrobin_part{N-1}),
                phân phối bản ghi đều đặn theo thứ tự vòng tròn.
    - Tác dụng: Đảm bảo phân bố đều dữ liệu, phù hợp cho các truy vấn không yêu cầu chọn lọc theo giá trị cụ thể.
    - Quy trình: Gán số thứ tự cho mỗi bản ghi bằng ROW_NUMBER(), sau đó phân phối bản ghi vào các bảng
                dựa trên công thức (row_number - 1) % N.

    Tham số:
    - ratingstablename (str): Tên bảng ratings.
    - numberofpartitions (int): Số lượng phân mảnh cần tạo (N).
    - openconnection: Đối tượng kết nối đến cơ sở dữ liệu PostgreSQL.

    Biến chính:
    - con: Đối tượng kết nối đến cơ sở dữ liệu.
    - cur: Con trỏ để thực thi lệnh SQL.
    - RROBIN_TABLE_PREFIX (str): Tiền tố tên bảng phân mảnh, mặc định là 'rrobin_part'.
    - table_name (str): Tên bảng phân mảnh (rrobin_part0, rrobin_part1, ...).
    """
    # Kiểm tra số phân mảnh hợp lệ
    if numberofpartitions <= 0:
        return  # Thoát hàm nếu N <= 0
    
    # con, cur: Kết nối và con trỏ SQL
    con = openconnection
    cur = con.cursor()
    
    # RROBIN_TABLE_PREFIX: Tiền tố để đặt tên bảng phân mảnh
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    # Lặp qua N phân mảnh để tạo bảng và chèn dữ liệu
    for i in range(numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)  # Tên bảng: rrobin_part0, rrobin_part1, ...
        
        # Tạo bảng phân mảnh với schema giống bảng ratings
        cur.execute(f"CREATE TABLE {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT);")
        
        # Chèn dữ liệu vào bảng phân mảnh theo phương pháp round-robin
        # ROW_NUMBER() OVER(): Gán số thứ tự (1, 2, 3, ...) cho mỗi bản ghi trong ratings
        # (rnum-1) % numberofpartitions = i: Chọn các bản ghi thuộc phân mảnh i
        cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) SELECT userid, movieid, rating FROM (SELECT userid, movieid, rating, ROW_NUMBER() OVER() AS rnum FROM {ratingstablename}) AS temp WHERE (rnum-1) % {numberofpartitions} = {i};")
    
    
    # Đếm số bản ghi hiện tại trong bảng ratings
    # total_rows: Số bản ghi sau khi chèn bản ghi mới
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
    total_rows = cur.fetchone()[0]
    
    cur.execute("CREATE TABLE roundrobin_metadata(next_use_partition int);")
    #Lưu lại vị trí tiếp theo để chèn
    next_use= int(total_rows%numberofpartitions)
    cur.execute(f"INSERT INTO roundrobin_metadata (next_use_partition) VALUES ({next_use});")
    cur.close()
    con.commit()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm chèn một bản ghi mới vào bảng ratings và bảng phân mảnh range tương ứng.
    - Mục đích: Thêm bản ghi vào bảng chính (ratings) và một bảng range_partX dựa trên giá trị rating.
    - Tác dụng: Duy trì tính toàn vẹn dữ liệu, đảm bảo bản ghi xuất hiện ở cả bảng chính và đúng phân mảnh.
    - Quy trình: Chèn bản ghi vào ratings, tính toán phân mảnh dựa trên rating / delta, chèn vào bảng range_partX.

    Tham số:
    - ratingstablename (str): Tên bảng ratings.
    - userid (int): ID của người dùng.
    - itemid (int): ID của phim (movieid).
    - rating (float): Điểm đánh giá, từ 0 đến 5.
    - openconnection: Đối tượng kết nối đến cơ sở dữ liệu PostgreSQL.

    Biến chính:
    - con: Đối tượng kết nối đến cơ sở dữ liệu.
    - cur: Con trỏ để thực thi lệnh SQL.
    - RANGE_TABLE_PREFIX (str): Tiền tố tên bảng phân mảnh ('range_part').
    - numberofpartitions (int): Số phân mảnh hiện có, lấy từ count_partitions.
    - delta (float): Độ rộng của mỗi khoảng rating (5.0 / numberofpartitions).
    - index (int): Chỉ số của bảng phân mảnh (X trong range_partX).
    - table_name (str): Tên bảng phân mảnh để chèn bản ghi.
    """
    # Kiểm tra rating có nằm trong khoảng hợp lệ (0 đến 5)
    if rating < 0 or rating > 5:
        return  # Thoát hàm nếu rating không hợp lệ
    
    # con, cur: Kết nối và con trỏ SQL
    con = openconnection
    cur = con.cursor()
    
    # Chèn bản ghi mới vào bảng ratings
    # userid, itemid, rating: Giá trị của bản ghi mới
    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES ({userid}, {itemid}, {rating});")
    
    # Tìm bảng phân mảnh range phù hợp
    RANGE_TABLE_PREFIX = 'range_part'
    # numberofpartitions: Số bảng phân mảnh hiện có, lấy từ hàm count_partitions
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    if numberofpartitions <= 0:
        return  # Thoát nếu không có bảng phân mảnh
    
    # Tính độ rộng khoảng rating và chỉ số phân mảnh
    delta = 5.0 / numberofpartitions
    index = int(rating / delta)  # Xác định phân mảnh dựa trên rating / delta
    # Điều chỉnh chỉ số nếu rating nằm ở ranh giới (ví dụ: rating=5 vào range_part{N-1})
    if rating % delta == 0 and index != 0:
        index -= 1
    table_name = RANGE_TABLE_PREFIX + str(index)  # Tên bảng: range_partX
    
    # Chèn bản ghi vào bảng phân mảnh tương ứng
    cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES ({userid}, {itemid}, {rating});")
    
    cur.close()
    con.commit()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm chèn một bản ghi mới vào bảng ratings và bảng phân mảnh round-robin tương ứng.
    - Mục đích: Thêm bản ghi vào bảng chính (ratings) và một bảng rrobin_partX dựa trên thứ tự chèn.
    - Tác dụng: Duy trì phân bố đều dữ liệu trong các phân mảnh round-robin.
    - Quy trình: Chèn bản ghi vào ratings, lấy phân mảnh tiếp theo trong metadata, chèn vào rrobin_partX.

    Tham số:
    - ratingstablename (str): Tên bảng ratings.
    - userid (int): ID của người dùng.
    - itemid (int): ID của phim (movieid).
    - rating (float): Điểm đánh giá, từ 0 đến 5.
    - openconnection: Đối tượng kết nối đến cơ sở dữ liệu PostgreSQL.

    Biến chính:
    - con: Đối tượng kết nối đến cơ sở dữ liệu.
    - cur: Con trỏ để thực thi lệnh SQL.
    - RROBIN_TABLE_PREFIX (str): Tiền tố tên bảng phân mảnh ('rrobin_part').
    - numberofpartitions (int): Số phân mảnh hiện có, lấy từ count_partitions.
    - total_rows (int): Số bản ghi hiện tại trong bảng ratings (sau khi chèn).
    - index (int): Chỉ số của bảng phân mảnh (X trong rrobin_partX).
    - table_name (str): Tên bảng phân mảnh để chèn bản ghi.
    """
    # Kiểm tra rating hợp lệ
    if rating < 0 or rating > 5:
        return  # Thoát hàm nếu rating không hợp lệ
    
    # con, cur: Kết nối và con trỏ SQL
    con = openconnection
    cur = con.cursor()
    
    # Chèn bản ghi mới vào bảng ratings
    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES ({userid}, {itemid}, {rating});")
    
    
    
    # Tìm bảng phân mảnh round-robin phù hợp
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    if numberofpartitions <= 0:
        return  # Thoát nếu không có bảng phân mảnh
    
    cur.execute("SELECT next_use_partition FROM roundrobin_metadata FOR UPDATE;")
    next_use = cur.fetchone()[0]
    cur.execute("UPDATE roundrobin_metadata SET next_use_partition=next_use_partition+1;")
    # Tính chỉ số phân mảnh dựa trên thứ tự chèn
    # index: (next_use) % numberofpartitions để chọn bảng theo vòng tròn
    index = (next_use) % numberofpartitions
    table_name = RROBIN_TABLE_PREFIX + str(index)  # Tên bảng: rrobin_partX
    
    # Chèn bản ghi vào bảng phân mảnh
    cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES ({userid}, {itemid}, {rating});")
    
    cur.close()
    con.commit()

def count_partitions(prefix, openconnection):
    """
    Hàm đếm số bảng phân mảnh có tên bắt đầu bằng tiền tố cho trước (range_part hoặc rrobin_part).
    - Mục đích: Xác định số phân mảnh hiện có để sử dụng trong rangeinsert và roundrobininsert.
    - Tác dụng: Hỗ trợ tính toán đúng phân mảnh khi chèn bản ghi mới.
    - Quy trình: Truy vấn bảng hệ thống pg_stat_user_tables để đếm số bảng có tên khớp với prefix.

    Tham số:
    - prefix (str): Tiền tố tên bảng (ví dụ: 'range_part', 'rrobin_part').
    - openconnection: Đối tượng kết nối đến cơ sở dữ liệu PostgreSQL.

    Biến:
    - con: Đối tượng kết nối đến cơ sở dữ liệu.
    - cur: Con trỏ để thực thi lệnh SQL.
    - count (int): Số lượng bảng có tên bắt đầu bằng prefix.
    """
    # con, cur: Kết nối và con trỏ SQL
    con = openconnection
    cur = con.cursor()
    
    # Truy vấn bảng hệ thống để đếm số bảng có tên bắt đầu bằng prefix
    # pg_stat_user_tables: Chứa thông tin về các bảng do người dùng tạo
    cur.execute(f"SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE '{prefix}%';")
    
    # Lấy kết quả đếm
    count = cur.fetchone()[0]
    cur.close()
    con.commit()
    # Trả về số lượng bảng phân mảnh
    return count
