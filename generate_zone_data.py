"""
구역별 시트 형태의 샘플 데이터 생성 및 Google Sheets 업로드
"""

import pandas as pd
import random
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import streamlit as st

# 한국 이름 목록
LAST_NAMES = ['김', '이', '박', '정', '최', '강', '조', '윤', '장', '임',
              '한', '오', '신', '권', '송', '유', '홍', '배', '노', '문',
              '서', '표', '남', '곽', '성', '황', '안', '전', '방', '주',
              '태', '탁', '석', '추', '변', '도']

FIRST_NAMES = ['민준', '지호', '예준', '도윤', '시우', '수빈', '하준',
               '정민', '지훈', '승현', '주원', '시현', '민재', '도현',
               '민규', '우진', '준서', '연우', '서윤', '수아', '하은',
               '서준', '지우', '민서', '예은', '지안', '채원', '서현',
               '다은', '은우', '서진', '예린', '지율', '서영', '아인', '유나']

def generate_zone_members(zone_num: int, num_members: int = 4):
    """구역별 멤버 생성 (리더 1명 + 청년 3명)"""
    members = []

    # 리더 1명
    leader_name = random.choice(LAST_NAMES) + random.choice(FIRST_NAMES)
    members.append({
        'name': leader_name,
        'position': '리더',
        'status': '재적'
    })

    # 청년 3명
    for _ in range(num_members - 1):
        youth_name = random.choice(LAST_NAMES) + random.choice(FIRST_NAMES)
        members.append({
            'name': youth_name,
            'position': '청년',
            'status': '재적'
        })

    return members

def generate_monthly_records(members: list, start_date: str, num_months: int = 3):
    """월별 레코드 생성"""
    records = []

    start = datetime.strptime(start_date, '%Y-%m-%d')

    for month_offset in range(num_months):
        # 각 월의 1일
        current_date = start + timedelta(days=30 * month_offset)
        current_date = current_date.replace(day=1)
        # 날짜 형식: "11월", "12월" 형식
        date_str = f"{current_date.month}월"

        for member in members:
            # 리더는 더 높은 이행률
            if member['position'] == '리더':
                attendance_prob = 0.95
                participation_prob = 0.9
            else:
                attendance_prob = 0.75
                participation_prob = 0.7

            record = {
                '날짜': date_str,
                '이름': member['name'],
                '직분': member['position'],
                '상태': member['status'],
                '전체출결': 1 if random.random() < attendance_prob else 0,
                '대면출결': 1 if random.random() < attendance_prob * 0.85 else 0,
                '마이심': 1 if random.random() < participation_prob else 0,
                '상시활동': 1 if random.random() < participation_prob * 0.8 else 0,
                '전도': 1 if random.random() < participation_prob * 0.75 else 0,
                '십일조': 1 if random.random() < participation_prob * 0.85 else 0,
            }

            records.append(record)

    df = pd.DataFrame(records)
    
    # 최신 월이 위로 오도록 정렬 (내림차순)
    if '날짜' in df.columns:
        df['_sort_key'] = df['날짜'].apply(
            lambda x: int(x.replace('월', '')) if isinstance(x, str) and '월' in x else 0
        )
        df = df.sort_values('_sort_key', ascending=False)
        df = df.drop(columns=['_sort_key'])
    
    return df

def upload_to_google_sheets():
    """Google Sheets에 구역별 시트로 데이터 업로드"""

    print("="*60)
    print("구역별 샘플 데이터 생성 및 Google Sheets 업로드")
    print("="*60)

    # Google Sheets 연결
    print("\n1️⃣ Google Sheets에 연결 중...")
    try:
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]

        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            st.secrets["gcp_service_account"],
            scope
        )

        client = gspread.authorize(credentials)
        print("   ✅ Google Sheets에 연결되었습니다.")
    except Exception as e:
        print(f"   ❌ 연결 실패: {e}")
        return

    # 스프레드시트 열기
    print("\n2️⃣ 스프레드시트 열기...")
    try:
        spreadsheet = client.open('남산 대시보드')
        print(f"   ✅ 스프레드시트를 찾았습니다: {spreadsheet.title}")
    except Exception as e:
        print(f"   ❌ 오류: {e}")
        return

    # 9개 구역 데이터 생성 및 업로드
    print("\n3️⃣ 구역별 데이터 생성 및 업로드 중...")

    total_records = 0

    for zone_num in range(1, 10):
        zone_name = f"{zone_num}구역"

        print(f"\n   [{zone_num}/9] {zone_name} 처리 중...")

        # 구역 멤버 생성
        members = generate_zone_members(zone_num, num_members=4)

        # 3개월치 레코드 생성
        zone_df = generate_monthly_records(members, '2024-09-01', num_months=3)

        total_records += len(zone_df)

        # 시트 확인 또는 생성
        try:
            worksheet = spreadsheet.worksheet(zone_name)
            print(f"      ✓ 기존 '{zone_name}' 시트를 찾았습니다.")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=zone_name, rows=100, cols=20)
            print(f"      ✓ '{zone_name}' 시트를 생성했습니다.")

        # 기존 데이터 삭제
        worksheet.clear()

        # 데이터 업로드
        data = [zone_df.columns.values.tolist()] + zone_df.values.tolist()
        worksheet.update(data, 'A1')

        print(f"      ✅ {len(zone_df)}개 레코드 업로드 완료")

    # 완료 메시지
    print("\n" + "="*60)
    print("✅ 모든 구역 데이터 업로드 완료!")
    print("="*60)
    print(f"\n📊 업로드 통계:")
    print(f"   - 구역 수: 9개")
    print(f"   - 총 레코드 수: {total_records}개")
    print(f"   - 기간: 2024년 9월 ~ 11월 (3개월)")
    print(f"\n🔗 스프레드시트 URL:")
    print(f"   {spreadsheet.url}")
    print(f"\n💡 이제 'streamlit run app_v2.py'를 실행하세요!")

if __name__ == "__main__":
    upload_to_google_sheets()
