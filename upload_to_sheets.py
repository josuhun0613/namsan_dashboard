"""
샘플 데이터를 Google Sheets에 업로드하는 스크립트
"""

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import streamlit as st

def upload_sample_data():
    """CSV 파일을 읽어서 Google Sheets에 업로드"""

    print("🔄 샘플 데이터를 Google Sheets에 업로드합니다...")

    # 1. CSV 파일 읽기
    print("\n1️⃣ CSV 파일 읽는 중...")
    try:
        df = pd.read_csv('sample_data.csv', encoding='utf-8-sig')
        print(f"   ✅ {len(df)}개의 레코드를 읽었습니다.")
    except FileNotFoundError:
        print("   ❌ sample_data.csv 파일이 없습니다.")
        print("   먼저 'python generate_sample_data.py'를 실행하세요.")
        return

    # 2. Google Sheets 연결
    print("\n2️⃣ Google Sheets에 연결 중...")
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
        print("   .streamlit/secrets.toml 파일을 확인하세요.")
        return

    # 3. 스프레드시트 열기
    print("\n3️⃣ 스프레드시트 열기...")
    try:
        spreadsheet = client.open('남산 대시보드')
        print(f"   ✅ 스프레드시트를 찾았습니다: {spreadsheet.title}")
    except gspread.SpreadsheetNotFound:
        print("   ❌ '남산 대시보드' 스프레드시트를 찾을 수 없습니다.")
        print("   Google Sheets에서 이 이름으로 스프레드시트를 생성하고")
        print("   서비스 계정과 공유했는지 확인하세요:")
        print("   namsandashboard@namsandashboard.iam.gserviceaccount.com")
        return
    except Exception as e:
        print(f"   ❌ 오류: {e}")
        return

    # 4. Record_DB 시트 선택 또는 생성
    print("\n4️⃣ 'Record_DB' 시트 확인...")
    try:
        worksheet = spreadsheet.worksheet('Record_DB')
        print("   ✅ 'Record_DB' 시트를 찾았습니다.")
    except gspread.WorksheetNotFound:
        print("   ⚠️  'Record_DB' 시트가 없습니다. 새로 생성합니다...")
        worksheet = spreadsheet.add_worksheet(title='Record_DB', rows=1000, cols=20)
        print("   ✅ 'Record_DB' 시트를 생성했습니다.")

    # 5. 기존 데이터 삭제
    print("\n5️⃣ 기존 데이터 삭제 중...")
    worksheet.clear()
    print("   ✅ 기존 데이터가 삭제되었습니다.")

    # 6. 새 데이터 업로드
    print("\n6️⃣ 새 데이터 업로드 중...")
    try:
        # 헤더 + 데이터를 리스트로 변환
        data = [df.columns.values.tolist()] + df.values.tolist()

        # 한 번에 업로드
        worksheet.update(data, 'A1')

        print(f"   ✅ {len(df)}개의 레코드가 업로드되었습니다!")
    except Exception as e:
        print(f"   ❌ 업로드 실패: {e}")
        return

    # 7. 업로드 결과 확인
    print("\n" + "="*60)
    print("✅ 업로드 완료!")
    print("="*60)
    print(f"\n📊 업로드된 데이터:")
    print(f"   - 총 레코드 수: {len(df)}개")
    print(f"   - 기간: {df['날짜'].min()} ~ {df['날짜'].max()}")
    print(f"   - 지역: {', '.join(df['지역'].unique())}")
    print(f"   - 구역: {', '.join(sorted(df['구역'].unique()))}")
    print(f"\n🔗 스프레드시트 URL:")
    print(f"   {spreadsheet.url}")
    print(f"\n💡 이제 'streamlit run app.py'를 실행하세요!")

if __name__ == "__main__":
    upload_sample_data()
