"""
초기 데이터 설정 스크립트
1. Record_DB: 전체 회원 마스터 DB 생성
2. 구역별 시트: 샘플 데이터 생성
"""

import pandas as pd
import random
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import streamlit as st

# 한국 이름 목록
LAST_NAMES = ['김', '이', '박', '정', '최', '강', '조', '윤', '장', '임',
              '한', '오', '신', '권', '송', '유', '홍', '배', '노', '문']

FIRST_NAMES = ['민준', '지호', '예준', '도윤', '시우', '수빈', '하준',
               '정민', '지훈', '승현', '주원', '시현', '민재', '도현',
               '서윤', '수아', '하은', '지우', '민서', '예은']

def generate_master_db(num_zones: int = 9, members_per_zone: int = 4):
    """전체 회원 마스터 DB 생성"""

    zone_region_map = {
        '1구역': '도원', '2구역': '도원', '3구역': '도원', '4구역': '도원',
        '5구역': '새신', '6구역': '새신', '7구역': '새신',
        '8구역': '청암', '9구역': '청암',
    }

    members = []

    for zone_num in range(1, num_zones + 1):
        zone_name = f"{zone_num}구역"
        region = zone_region_map.get(zone_name, '도원')

        # 리더 1명
        leader_name = random.choice(LAST_NAMES) + random.choice(FIRST_NAMES)
        members.append({
            '이름': leader_name,
            '지역': region,
            '구역': zone_name,
            '직분': '리더',
            '상태': '재적',
            '입회일': '2024-01-01'
        })

        # 청년 3명
        for _ in range(members_per_zone - 1):
            youth_name = random.choice(LAST_NAMES) + random.choice(FIRST_NAMES)
            members.append({
                '이름': youth_name,
                '지역': region,
                '구역': zone_name,
                '직분': '청년',
                '상태': '재적',
                '입회일': '2024-01-01'
            })

    return pd.DataFrame(members)

def setup_initial_data():
    """초기 데이터 설정"""

    print("="*60)
    print("청년회 대시보드 초기 데이터 설정")
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
        print("   ✅ 연결 성공")
    except Exception as e:
        print(f"   ❌ 연결 실패: {e}")
        return

    # 스프레드시트 열기
    print("\n2️⃣ 스프레드시트 열기...")
    try:
        spreadsheet = client.open('남산 대시보드')
        print(f"   ✅ '{spreadsheet.title}' 열림")
    except Exception as e:
        print(f"   ❌ 오류: {e}")
        return

    # 마스터 DB 생성
    print("\n3️⃣ Record_DB (마스터 DB) 생성 중...")
    master_df = generate_master_db(num_zones=9, members_per_zone=4)

    try:
        try:
            worksheet = spreadsheet.worksheet('Record_DB')
            print("   ✓ 기존 Record_DB 시트 발견")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title='Record_DB', rows=1000, cols=20)
            print("   ✓ Record_DB 시트 생성")

        worksheet.clear()
        data = [master_df.columns.values.tolist()] + master_df.values.tolist()
        worksheet.update(data, 'A1')

        print(f"   ✅ {len(master_df)}명의 회원 데이터 업로드 완료")
    except Exception as e:
        print(f"   ❌ 오류: {e}")
        return

    # 구역별 시트는 비워두기 (앱에서 '월 생성' 기능 사용)
    print("\n4️⃣ 구역별 시트 생성...")

    for zone_num in range(1, 10):
        zone_name = f"{zone_num}구역"

        try:
            try:
                worksheet = spreadsheet.worksheet(zone_name)
                print(f"   ✓ {zone_name} 시트 이미 존재")
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=zone_name, rows=1000, cols=20)
                print(f"   ✓ {zone_name} 시트 생성")

            # 헤더만 추가
            headers = ['날짜', '이름', '직분', '상태', '전체출결', '대면출결', '마이심', '상시활동', '전도', '십일조']
            worksheet.clear()
            worksheet.update([headers], 'A1')

        except Exception as e:
            print(f"   ❌ {zone_name} 오류: {e}")

    # 완료
    print("\n" + "="*60)
    print("✅ 초기 데이터 설정 완료!")
    print("="*60)
    print(f"\n📊 설정된 데이터:")
    print(f"   - Record_DB: {len(master_df)}명의 회원")
    print(f"   - 구역 시트: 9개 (헤더만 설정)")
    print(f"\n🔗 스프레드시트 URL:")
    print(f"   {spreadsheet.url}")
    print(f"\n💡 다음 단계:")
    print(f"   1. streamlit run app_final.py")
    print(f"   2. '📅 월 생성' 탭에서 원하는 월 생성")
    print(f"   3. '✏️ 데이터 입력' 탭에서 데이터 입력")
    print(f"   4. '📊 성적표 보기' 탭에서 결과 확인")

if __name__ == "__main__":
    setup_initial_data()
