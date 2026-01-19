from datetime import date, datetime, timedelta, date
from dateutil.relativedelta import relativedelta


quarter_months = [3, 6, 9, 12]
month_map = {10: "A", 11: "B", 12: "C"}
first_derivative_year = 2020
year_map = "ABCDEFGHJKLMNPQRSTVWX"


def find_third_thursday(year, month):
    d = date(year, month, 1)
    # tìm thứ Năm đầu tiên
    while d.weekday() != 3:  # Monday=0, Thursday=3
        d += timedelta(days=1)
    # cộng thêm 14 ngày => thứ Năm lần 3
    return d + timedelta(days=14)


def get_list_derivative_due_month(base_date: date):
    thurs_3rd = find_third_thursday(base_date.year, base_date.month)
    due_1m = date(base_date.year, base_date.month, 1) if base_date <= thurs_3rd else date(base_date.year, base_date.month, 1) + relativedelta(months=1)
    due_2m = due_1m + relativedelta(months=1)
    due_qm = []

    temp_m = due_2m
    while len(due_qm) < 2:
        temp_m += relativedelta(months=1)
        if temp_m.month in quarter_months:
            due_qm.append(temp_m)
    output = [due_1m, due_2m]
    output.extend(due_qm)
    return output


def krx_code(base_date: date):
    year_code = year_map[(base_date.year - first_derivative_year) % len(year_map)]
    month_code = month_map.get(base_date.month, base_date.month)
    return f"41I1{year_code}{month_code}000"


def get_derivative_underlying_codes(base_date: date):
    due_months = get_list_derivative_due_month(base_date)
    return {
        "VN30F1M": krx_code(due_months[0]),
        "VN30F2M": krx_code(due_months[1]),
        "VN30F1Q": krx_code(due_months[2]),
        "VN30F2Q": krx_code(due_months[3])
    }


if __name__ == "__main__":
    cur_date = date(2026, 1, 19)
    print(get_derivative_underlying_codes(cur_date))

# list_due = get_list_derivative_due_month(cur_date)
#
# print(f"Tháng đáo hạn hiện tại: {list_due[0]}")
# print(f"Các mã phái sinh VN30 cũ đang niêm yết là: {[d.strftime('VN30F%y%m') for d in list_due]}")
# print(f"Các mã phái sinh VN30 mới đang niêm yết là: {[krx_code(d) for d in list_due]}")
