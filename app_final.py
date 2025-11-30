"""
청년회 사역 관리 대시보드 - 최종 통합 버전
- Record_DB: 전체 회원 마스터 DB
- 구역별 시트: 월별 출석/활동 기록
- 월 생성 기능: 새로운 월 자동 생성
- 자동 동기화: 구역 변동 시 자동 반영
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import plotly.graph_objects as go

# ==================== 페이지 설정 ====================
st.set_page_config(
    page_title="청년회 사역 관리 대시보드",
    page_icon="⛪",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== Google Sheets 연결 ====================
@st.cache_resource
def connect_to_gsheet():
    """Google Sheets API 연결"""
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
        return client
    except Exception as e:
        st.error(f"Google Sheets 연결 실패: {str(e)}")
        return None

@st.cache_data(ttl=60)
def load_master_db(_client) -> pd.DataFrame:
    """Record_DB 시트에서 전체 회원 마스터 데이터 로드"""
    try:
        spreadsheet = _client.open('남산 대시보드')
        worksheet = spreadsheet.worksheet('Record_DB')

        data = worksheet.get_all_records()
        if not data:
            return pd.DataFrame(columns=['이름', '지역', '구역', '직분', '상태', '입회일'])

        df = pd.DataFrame(data)
        return df
    except gspread.WorksheetNotFound:
        st.warning("Record_DB 시트가 없습니다. 새로 생성합니다.")
        return pd.DataFrame(columns=['이름', '지역', '구역', '직분', '상태', '입회일'])
    except Exception as e:
        st.error(f"마스터 DB 로드 실패: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_zone_data(_client, zone_name: str) -> pd.DataFrame:
    """특정 구역 시트 데이터 로드"""
    try:
        spreadsheet = _client.open('남산 대시보드')
        worksheet = spreadsheet.worksheet(zone_name)

        data = worksheet.get_all_records()
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        if '날짜' in df.columns:
            # 날짜는 "11월", "12월" 형식의 문자열로 유지
            # 빈 값 제거
            df = df[df['날짜'].notna() & (df['날짜'] != '')]

        return df
    except gspread.WorksheetNotFound:
        return pd.DataFrame()
    except Exception as e:
        st.error(f"{zone_name} 데이터 로드 실패: {str(e)}")
        return pd.DataFrame()

def get_available_months(_client, all_zones: tuple) -> list:
    """모든 구역에서 사용 가능한 월 목록 추출"""
    all_dates = set()  # 중복 제거를 위해 set 사용

    try:
        spreadsheet = _client.open('남산 대시보드')

        # 모든 구역을 확인하되, 에러가 나도 계속 진행
        import time as time_module

        for idx, zone in enumerate(all_zones):
            if pd.isna(zone) or zone == '':
                continue

            try:
                worksheet = spreadsheet.worksheet(zone)
                data = worksheet.get_all_records()

                if data:
                    df = pd.DataFrame(data)
                    if '날짜' in df.columns:
                        # 날짜는 "11월", "12월" 형식의 문자열
                        valid_dates = df['날짜'].dropna()
                        
                        # "11월" 형식인 값들만 추출
                        for date_val in valid_dates:
                            if isinstance(date_val, str) and '월' in date_val:
                                all_dates.add(date_val)

                # API 호출 속도 제한 방지 (매 3개 구역마다 0.5초 대기)
                if (idx + 1) % 3 == 0:
                    time_module.sleep(0.5)

            except gspread.WorksheetNotFound:
                continue
            except Exception as e:
                # API 할당량 초과 등의 에러는 무시하고 계속 진행
                continue

        if not all_dates:
            return []

        # set을 월 숫자 기준으로 정렬 (최신순)
        sorted_months = sorted(list(all_dates), key=lambda x: int(x.replace('월', '')), reverse=True)
        return sorted_months

    except Exception as e:
        st.error(f"월 목록 로드 실패: {str(e)}")
        return []

def save_master_db(client, df: pd.DataFrame) -> bool:
    """Record_DB에 마스터 데이터 저장"""
    try:
        spreadsheet = client.open('남산 대시보드')

        try:
            worksheet = spreadsheet.worksheet('Record_DB')
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title='Record_DB', rows=1000, cols=20)

        worksheet.clear()
        data = [df.columns.values.tolist()] + df.values.tolist()
        worksheet.update(data, 'A1')

        return True
    except Exception as e:
        st.error(f"마스터 DB 저장 실패: {str(e)}")
        return False

def save_zone_data(client, zone_name: str, df: pd.DataFrame) -> bool:
    """특정 구역 시트에 데이터 저장"""
    try:
        spreadsheet = client.open('남산 대시보드')

        try:
            worksheet = spreadsheet.worksheet(zone_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=zone_name, rows=1000, cols=20)

        # 날짜는 이미 "11월" 형식의 문자열이므로 그대로 사용
        df_copy = df.copy()

        worksheet.clear()
        data = [df_copy.columns.values.tolist()] + df_copy.values.tolist()
        worksheet.update(data, 'A1')

        return True
    except Exception as e:
        st.error(f"{zone_name} 저장 실패: {str(e)}")
        return False

def create_monthly_records(client, master_df: pd.DataFrame, target_month: str, region_filter: str = '전체') -> bool:
    """
    새로운 월의 데이터 폼 생성
    마스터 DB를 기반으로 각 구역 시트에 해당 월의 빈 레코드 추가
    region_filter: '전체', '도원', '새신', '청암' 중 선택
    target_month: "11월", "12월" 형식
    """
    try:
        # 대상 월 형식 확인 및 정규화 (예: "2024년 11월" → "11월" 또는 "11월" → "11월")
        if '년' in target_month:
            # "2024년 11월" 형식인 경우
            month = target_month.split('년 ')[1].replace('월', '').strip()
            date_str = f"{month}월"
        else:
            # "11월" 형식인 경우
            date_str = target_month if target_month.endswith('월') else f"{target_month}월"

        # 지역 필터 적용
        if region_filter != '전체':
            filtered_df = master_df[master_df['지역'] == region_filter]
        else:
            filtered_df = master_df

        # 구역별로 그룹화
        zones = filtered_df['구역'].unique()

        success_count = 0

        for zone in zones:
            if pd.isna(zone) or zone == '':
                continue

            # 해당 구역의 재적 인원만 필터링
            zone_members = filtered_df[
                (filtered_df['구역'] == zone) &
                (filtered_df['상태'] == '재적')
            ]

            if zone_members.empty:
                continue

            # 기존 구역 데이터 로드
            existing_df = load_zone_data(client, zone)

            # 새로운 레코드 생성
            new_records = []
            for _, member in zone_members.iterrows():
                record = {
                    '날짜': date_str,
                    '이름': member['이름'],
                    '직분': member['직분'],
                    '상태': member['상태'],
                    '전체출결': 0,
                    '대면출결': 0,
                    '마이심': 0,
                    '상시활동': 0,
                    '전도': 0,
                    '십일조': 0
                }
                new_records.append(record)

            new_df = pd.DataFrame(new_records)

            # 기존 데이터와 병합 (최신 데이터가 위로 오도록)
            if not existing_df.empty:
                combined_df = pd.concat([new_df, existing_df], ignore_index=True)
            else:
                combined_df = new_df

            # 날짜 기준으로 정렬 (최신 월이 위로 오도록 내림차순)
            if '날짜' in combined_df.columns:
                # 월 숫자 기준으로 정렬 (예: "12월" > "11월" > "10월")
                combined_df['_sort_key'] = combined_df['날짜'].apply(
                    lambda x: int(x.replace('월', '')) if isinstance(x, str) and '월' in x else 0
                )
                combined_df = combined_df.sort_values('_sort_key', ascending=False)
                combined_df = combined_df.drop(columns=['_sort_key'])

            # 저장
            if save_zone_data(client, zone, combined_df):
                success_count += 1

        return success_count > 0

    except Exception as e:
        st.error(f"월 생성 실패: {str(e)}")
        return False

def sync_member_to_zones(client, master_df: pd.DataFrame, member_name: str, new_zone: str, old_zone: str = None):
    """
    회원의 구역 변동을 구역 시트에 반영
    - old_zone: 이전 구역 (있으면 해당 구역 시트에서 상태를 '제외'로 변경)
    - new_zone: 새로운 구역 (해당 구역 시트에 회원 추가)
    """
    try:
        member_info = master_df[master_df['이름'] == member_name].iloc[0]

        # 이전 구역에서 제외 처리
        if old_zone and old_zone != new_zone:
            old_df = load_zone_data(client, old_zone)
            if not old_df.empty:
                old_df.loc[old_df['이름'] == member_name, '상태'] = '제외'
                save_zone_data(client, old_zone, old_df)

        # 새 구역에 추가 (가장 최근 월 데이터에)
        new_df = load_zone_data(client, new_zone)

        if not new_df.empty and '날짜' in new_df.columns:
            # 가장 최근 날짜 찾기 (월 숫자 기준)
            new_df['_sort_key'] = new_df['날짜'].apply(
                lambda x: int(x.replace('월', '')) if isinstance(x, str) and '월' in x else 0
            )
            latest_date = new_df.loc[new_df['_sort_key'].idxmax(), '날짜']
            new_df = new_df.drop(columns=['_sort_key'])

            # 해당 날짜에 이미 있는지 확인
            existing = new_df[
                (new_df['날짜'] == latest_date) &
                (new_df['이름'] == member_name)
            ]

            if existing.empty:
                # 새로운 레코드 추가
                new_record = {
                    '날짜': latest_date,
                    '이름': member_name,
                    '직분': member_info['직분'],
                    '상태': '재적',
                    '전체출결': 0,
                    '대면출결': 0,
                    '마이심': 0,
                    '상시활동': 0,
                    '전도': 0,
                    '십일조': 0
                }
                new_df = pd.concat([new_df, pd.DataFrame([new_record])], ignore_index=True)
                save_zone_data(client, new_zone, new_df)

        return True

    except Exception as e:
        st.error(f"회원 동기화 실패: {str(e)}")
        return False

def calculate_zone_summary(zone_df: pd.DataFrame, month: str, zone_name: str, region: str) -> Dict:
    """구역별 월간 요약 통계 계산"""
    if zone_df.empty:
        return None

    # 해당 월 데이터 필터링 (날짜는 "11월" 형식)
    month_df = zone_df[zone_df['날짜'] == month]

    if month_df.empty:
        return None

    # 재적 인원만 필터링
    active_df = month_df[month_df['상태'] == '재적']

    if active_df.empty:
        return None

    # 구역장 찾기
    leader = active_df[active_df['직분'].isin(['리더', '구역장', '간사'])]['이름'].values
    leader_name = leader[0] if len(leader) > 0 else '-'

    # 통계 계산
    total_members = len(active_df)

    indicators = {
        '전체출결': active_df['전체출결'].sum() / total_members * 100 if total_members > 0 else 0,
        '대면출결': active_df['대면출결'].sum() / total_members * 100 if total_members > 0 else 0,
        '마이심': active_df['마이심'].sum() / total_members * 100 if total_members > 0 else 0,
        '상시활동': active_df['상시활동'].sum() / total_members * 100 if total_members > 0 else 0,
        '전도': active_df['전도'].sum() / total_members * 100 if total_members > 0 else 0,
        '십일조': active_df['십일조'].sum() / total_members * 100 if total_members > 0 else 0,
    }

    return {
        '지역': region,
        '구역': zone_name,
        '구역장': leader_name,
        '재적': total_members,
        '전체출결': round(indicators['전체출결']),
        '대면출결': round(indicators['대면출결']),
        '마이심': round(indicators['마이심']),
        '상시활동': round(indicators['상시활동']),
        '전도': round(indicators['전도']),
        '한자율': round(indicators['전도']),
        '십일조': round(indicators['십일조']),
    }

def calculate_comprehensive_score(row: pd.Series, weights: Dict[str, float]) -> float:
    """종합지표 계산"""
    indicators = ['전체출결', '대면출결', '마이심', '상시활동', '전도', '십일조']

    score = 0.0
    for ind in indicators:
        if ind in row and ind in weights:
            score += row[ind] * (weights[ind] / 100)

    return round(score, 1)

# ==================== UI 렌더링 ====================
def render_sidebar(client, master_df: pd.DataFrame) -> Tuple[str, Dict]:
    """사이드바 렌더링"""
    st.sidebar.title("⛪ 청년회 대시보드")
    st.sidebar.markdown("---")

    # 탭 선택
    tab_selection = st.sidebar.radio(
        "메뉴",
        ["📊 지표 보기", "🎯 구역별 지표보기", "👥 회원 관리", "📅 월 생성", "✏️ 데이터 입력"],
        index=0
    )

    st.sidebar.markdown("---")

    # 가중치 설정
    with st.sidebar.expander("⚖️ 종합지표 가중치"):
        indicators = ['전체출결', '대면출결', '마이심', '상시활동', '전도', '십일조']
        default_weight = 100 / len(indicators)

        weights = {}
        for ind in indicators:
            weights[ind] = st.slider(
                ind,
                min_value=0.0,
                max_value=100.0,
                value=default_weight,
                step=5.0,
                key=f"weight_{ind}"
            )

        total_weight = sum(weights.values())
        if abs(total_weight - 100) > 0.1:
            st.warning(f"⚠️ 합계: {total_weight:.1f}%")
        else:
            st.success(f"✅ 합계: {total_weight:.1f}%")

    return tab_selection, weights

def render_scoreboard_tab(client, master_df: pd.DataFrame, weights: Dict):
    """지표 보기 탭"""
    st.title("📊 청년회 지표")

    # 새로고침 버튼
    col1, col2 = st.columns([5, 1])
    with col2:
        if st.button("🔄 새로고침", key="refresh_scoreboard", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # 사용 가능한 월 목록 생성 (2분 캐시)
    all_zones = master_df['구역'].dropna().unique()
    available_months = get_available_months(client, tuple(all_zones))

    if not available_months:
        st.warning("데이터가 없습니다. '월 생성' 탭에서 새로운 월을 생성하세요.")
        return

    with col1:
        selected_month = st.selectbox("📅 기준 월", available_months, index=0)

    st.markdown("---")

    # 구역별 요약 데이터 생성
    zone_region_map = {
        '1구역': '도원', '2구역': '도원', '3구역': '도원', '4구역': '도원',
        '5구역': '새신', '6구역': '새신', '7구역': '새신',
        '8구역': '청암', '9구역': '청암',
    }

    summary_list = []

    for zone in all_zones:
        zone_df = load_zone_data(client, zone)
        if zone_df.empty:
            continue

        region = zone_region_map.get(zone, '기타')
        summary = calculate_zone_summary(zone_df, selected_month, zone, region)

        if summary:
            summary_list.append(summary)

    if not summary_list:
        st.warning(f"{selected_month}에 대한 데이터가 없습니다.")
        return

    # DataFrame 생성
    summary_df = pd.DataFrame(summary_list)

    # 종합지표 계산
    summary_df['종합지표'] = summary_df.apply(
        lambda row: calculate_comprehensive_score(row, weights),
        axis=1
    )

    # 순위 계산
    summary_df = summary_df.sort_values('종합지표', ascending=False)
    summary_df['순위'] = range(1, len(summary_df) + 1)

    # 지역별 표시
    regions = summary_df['지역'].unique()

    for region in sorted(regions):
        region_df = summary_df[summary_df['지역'] == region].copy()

        if region_df.empty:
            continue

        st.subheader(f"📍 {region} 지역")

        # 표시할 컬럼
        display_columns = ['구역', '구역장', '재적', '전체출결', '대면출결',
                          '마이심', '상시활동', '전도', '한자율', '십일조', '종합지표', '순위']

        display_df = region_df[display_columns].copy()

        # 퍼센트 문자열로 변환
        percent_columns = ['전체출결', '대면출결', '마이심', '상시활동', '전도', '한자율', '십일조', '종합지표']
        for col in percent_columns:
            display_df[col] = display_df[col].apply(lambda x: f"{x}%")

        # 지역 총합 행 추가
        total_row = {
            '구역': f'{region}지역 총합',
            '구역장': '',
            '재적': int(region_df['재적'].sum()),
            '전체출결': f"{int(region_df['전체출결'].mean())}%",
            '대면출결': f"{int(region_df['대면출결'].mean())}%",
            '마이심': f"{int(region_df['마이심'].mean())}%",
            '상시활동': f"{int(region_df['상시활동'].mean())}%",
            '전도': f"{int(region_df['전도'].mean())}%",
            '한자율': f"{int(region_df['한자율'].mean())}%",
            '십일조': f"{int(region_df['십일조'].mean())}%",
            '종합지표': '',
            '순위': ''
        }

        # 총합 행을 DataFrame에 추가
        display_df = pd.concat([display_df, pd.DataFrame([total_row])], ignore_index=True)

        # 표시
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # 실제 숫자 행 추가 (이미지처럼)
        actual_numbers = {
            '전체출결': int(region_df['재적'].sum() * region_df['전체출결'].mean() / 100),
            '대면출결': int(region_df['재적'].sum() * region_df['대면출결'].mean() / 100),
            '마이심': int(region_df['재적'].sum() * region_df['마이심'].mean() / 100),
            '상시활동': int(region_df['재적'].sum() * region_df['상시활동'].mean() / 100),
            '전도': int(region_df['재적'].sum() * region_df['전도'].mean() / 100),
            '십일조': int(region_df['재적'].sum() * region_df['십일조'].mean() / 100),
        }

        st.caption(f"📊 실제 인원: 전체출결 {actual_numbers['전체출결']}명 | "
                  f"대면출결 {actual_numbers['대면출결']}명 | "
                  f"마이심 {actual_numbers['마이심']}명 | "
                  f"상시활동 {actual_numbers['상시활동']}명 | "
                  f"전도 {actual_numbers['전도']}명 | "
                  f"십일조 {actual_numbers['십일조']}명")

        st.markdown("---")

def render_member_management_tab(client, master_df: pd.DataFrame):
    """회원 관리 탭"""
    st.title("👥 회원 관리")

    st.info("💡 Record_DB 시트: 전체 회원 마스터 데이터베이스입니다. 여기서 구역을 변경하면 자동으로 구역 시트에 반영됩니다.")

    # 데이터 편집
    edited_df = st.data_editor(
        master_df,
        use_container_width=True,
        num_rows="dynamic",
        hide_index=True,
        column_config={
            "이름": st.column_config.TextColumn("이름", required=True),
            "지역": st.column_config.SelectboxColumn(
                "지역",
                options=["도원", "새신", "청암"],
                required=True
            ),
            "구역": st.column_config.TextColumn("구역", required=True),
            "직분": st.column_config.SelectboxColumn(
                "직분",
                options=["청년", "리더", "구역장", "간사"],
                required=True
            ),
            "상태": st.column_config.SelectboxColumn(
                "상태",
                options=["재적", "제외"],
                required=True
            ),
            "입회일": st.column_config.TextColumn("입회일")
        }
    )

    col1, col2 = st.columns([1, 5])

    with col1:
        if st.button("💾 저장", type="primary", use_container_width=True):
            with st.spinner("저장 중..."):
                # 변경 사항 감지 및 동기화
                if not master_df.equals(edited_df):
                    # 구역 변경된 회원 찾기
                    for idx, row in edited_df.iterrows():
                        if idx < len(master_df):
                            old_row = master_df.iloc[idx]
                            if 'name' in row and row['이름'] == old_row['이름']:
                                if row['구역'] != old_row['구역']:
                                    # 구역 변경 감지
                                    sync_member_to_zones(
                                        client,
                                        edited_df,
                                        row['이름'],
                                        row['구역'],
                                        old_row['구역']
                                    )

                # 마스터 DB 저장
                if save_master_db(client, edited_df):
                    st.success("✅ 저장 완료! 구역 변경 사항이 반영되었습니다.")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("❌ 저장 실패")

    with col2:
        st.caption("⚠️ 구역을 변경하면 해당 회원이 새로운 구역 시트로 이동합니다.")

def render_monthly_generation_tab(client, master_df: pd.DataFrame):
    """월 생성 탭"""
    st.title("📅 월 데이터 생성")

    st.info("💡 새로운 월의 데이터 입력 폼을 자동으로 생성합니다. Record_DB의 재적 회원을 기준으로 각 구역 시트에 빈 레코드가 추가됩니다.")

    # 현재 년월
    current_date = datetime.now()

    col1, col2, col3 = st.columns(3)

    with col1:
        year = st.number_input("연도", min_value=2020, max_value=2030, value=current_date.year)

    with col2:
        month = st.number_input("월", min_value=1, max_value=12, value=current_date.month)

    with col3:
        region_filter = st.selectbox("생성할 지역", ["전체", "도원", "새신", "청암"])

    target_month = f"{month}월"

    st.markdown("---")

    st.subheader(f"생성할 월: {target_month} ({region_filter})")

    # 재적 인원 미리보기 (지역 필터 적용)
    active_members = master_df[master_df['상태'] == '재적']

    if region_filter != '전체':
        active_members = active_members[active_members['지역'] == region_filter]

    st.write(f"**재적 인원**: {len(active_members)}명")

    zone_counts = active_members['구역'].value_counts().sort_index()

    col1, col2, col3 = st.columns(3)

    for idx, (zone, count) in enumerate(zone_counts.items()):
        with [col1, col2, col3][idx % 3]:
            st.metric(zone, f"{count}명")

    st.markdown("---")

    if st.button(f"🚀 {region_filter} 월 생성", type="primary", use_container_width=False):
        with st.spinner(f"{target_month} {region_filter} 데이터 생성 중..."):
            if create_monthly_records(client, master_df, target_month, region_filter):
                st.success(f"✅ {target_month} {region_filter} 데이터가 성공적으로 생성되었습니다!")
                st.cache_data.clear()
                time.sleep(1)
                st.rerun()
            else:
                st.error("❌ 월 생성 실패")

def render_data_input_tab(client, master_df: pd.DataFrame):
    """데이터 입력 탭"""
    st.title("✏️ 데이터 입력")

    # 새로고침 버튼
    col1, col2 = st.columns([5, 1])
    with col2:
        if st.button("🔄 새로고침", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # 구역 선택
    all_zones = sorted(master_df['구역'].dropna().unique())

    if not all_zones:
        st.warning("회원 관리에서 먼저 회원을 등록하세요.")
        return

    with col1:
        selected_zone = st.selectbox("📍 구역 선택", all_zones)

    # 해당 구역 데이터 로드
    zone_df = load_zone_data(client, selected_zone)

    if zone_df.empty:
        st.warning(f"{selected_zone}에 데이터가 없습니다. '월 생성' 탭에서 월을 먼저 생성하세요.")
        return

    # 월 선택 (날짜는 "11월" 형식)
    available_months = sorted(zone_df['날짜'].unique(), key=lambda x: int(x.replace('월', '')) if isinstance(x, str) and '월' in x else 0, reverse=True)

    if not available_months:
        st.warning(f"{selected_zone}에 유효한 날짜 데이터가 없습니다.")
        return

    selected_month = st.selectbox("📅 월 선택", available_months)

    # 해당 월 데이터 필터링
    month_df = zone_df[zone_df['날짜'] == selected_month].copy()

    if month_df.empty:
        st.warning(f"{selected_month} 데이터가 없습니다.")
        return

    st.markdown("---")

    # 데이터 입력
    st.subheader(f"{selected_zone} - {selected_month}")

    edited_df = st.data_editor(
        month_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "날짜": st.column_config.TextColumn("날짜", disabled=True),
            "이름": st.column_config.TextColumn("이름", disabled=True),
            "직분": st.column_config.TextColumn("직분", disabled=True),
            "상태": st.column_config.SelectboxColumn(
                "상태",
                options=["재적", "제외"]
            ),
            "전체출결": st.column_config.NumberColumn("전체출결", min_value=0, max_value=1),
            "대면출결": st.column_config.NumberColumn("대면출결", min_value=0, max_value=1),
            "마이심": st.column_config.NumberColumn("마이심", min_value=0, max_value=1),
            "상시활동": st.column_config.NumberColumn("상시활동", min_value=0, max_value=1),
            "전도": st.column_config.NumberColumn("전도", min_value=0, max_value=1),
            "십일조": st.column_config.NumberColumn("십일조", min_value=0, max_value=1),
        }
    )

    col1, col2 = st.columns([1, 5])

    with col1:
        if st.button("💾 저장", type="primary", use_container_width=True):
            with st.spinner("저장 중..."):
                # 전체 zone_df에서 해당 월 데이터만 업데이트
                zone_df_updated = zone_df[zone_df['날짜'] != selected_month].copy()

                # 수정된 데이터 추가
                zone_df_updated = pd.concat([zone_df_updated, edited_df], ignore_index=True)

                # 날짜 기준으로 정렬 (최신 월이 위로 오도록 내림차순)
                zone_df_updated['_sort_key'] = zone_df_updated['날짜'].apply(
                    lambda x: int(x.replace('월', '')) if isinstance(x, str) and '월' in x else 0
                )
                zone_df_updated = zone_df_updated.sort_values('_sort_key', ascending=False)
                zone_df_updated = zone_df_updated.drop(columns=['_sort_key'])

                if save_zone_data(client, selected_zone, zone_df_updated):
                    st.success("✅ 저장 완료!")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("❌ 저장 실패")

    with col2:
        st.caption("💡 0 = 미이행, 1 = 이행")

def render_zone_detail_tab(client, master_df: pd.DataFrame, weights: Dict):
    """구역별 지표보기 탭 - 레이더 차트"""
    st.title("🎯 구역별 상세 지표")

    # 새로고침 버튼
    col_refresh1, col_refresh2 = st.columns([5, 1])
    with col_refresh2:
        if st.button("🔄 새로고침", key="refresh_zone_detail", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # 구역 목록
    all_zones = sorted([z for z in master_df['구역'].dropna().unique() if '구역' in str(z)])

    if not all_zones:
        st.warning("구역 데이터가 없습니다.")
        return

    # 사용 가능한 월 목록 생성 (2분 캐시)
    available_months = get_available_months(client, tuple(all_zones))

    if not available_months:
        st.warning("데이터가 없습니다. '월 생성' 탭에서 새로운 월을 생성하세요.")
        return

    # 선택 UI
    col1, col2 = st.columns(2)

    with col1:
        selected_zone = st.selectbox("📍 구역 선택", all_zones, index=0)

    with col2:
        selected_month = st.selectbox("📅 기준 월", available_months, index=0)

    st.markdown("---")

    # 구역 데이터 로드
    zone_df = load_zone_data(client, selected_zone)

    if zone_df.empty:
        st.warning(f"{selected_zone} 데이터가 없습니다.")
        return

    # 지역 매핑
    zone_region_map = {
        '1구역': '도원', '2구역': '도원', '3구역': '도원', '4구역': '도원',
        '5구역': '새신', '6구역': '새신', '7구역': '새신',
        '8구역': '청암', '9구역': '청암',
    }

    region = zone_region_map.get(selected_zone, '기타')

    # 현재 월 데이터 계산
    current_summary = calculate_zone_summary(zone_df, selected_month, selected_zone, region)

    if current_summary is None:
        st.warning(f"{selected_month}에 대한 {selected_zone} 데이터가 없습니다.")
        return

    # 이전 월 계산 (날짜는 "11월" 형식)
    current_month_num = int(selected_month.replace('월', ''))
    previous_month_num = current_month_num - 1
    if previous_month_num < 1:
        previous_month_num = 12
    previous_month = f"{previous_month_num}월"

    previous_summary = None
    if previous_month in available_months:
        previous_summary = calculate_zone_summary(zone_df, previous_month, selected_zone, region)

    # 종합지표 계산
    current_summary['종합지표'] = calculate_comprehensive_score(current_summary, weights)

    if previous_summary:
        previous_summary['종합지표'] = calculate_comprehensive_score(previous_summary, weights)

    # 지표 테이블 표시
    st.subheader(f"📊 {selected_zone} ({region}) - {selected_month}")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("구역장", current_summary['구역장'])

    with col2:
        st.metric("재적 인원", f"{current_summary['재적']}명")

    with col3:
        st.metric("종합지표", f"{current_summary['종합지표']}%")

    st.markdown("---")

    # 지표별 상세 데이터
    indicators = ['전체출결', '대면출결', '마이심', '상시활동', '전도', '십일조']

    cols = st.columns(6)
    for i, ind in enumerate(indicators):
        with cols[i]:
            current_val = current_summary[ind]
            if previous_summary:
                prev_val = previous_summary[ind]
                delta = current_val - prev_val
                st.metric(ind, f"{current_val}%", delta=f"{delta:+.0f}%")
            else:
                st.metric(ind, f"{current_val}%")

    st.markdown("---")

    # 레이더 차트 생성
    st.subheader("📈 지표 레이더 차트")

    categories = ['전체출결', '대면출결', '마이심', '상시활동', '전도', '십일조']

    # 현재 월 데이터
    current_values = [current_summary[cat] for cat in categories]

    fig = go.Figure()

    # 현재 월 트레이스
    fig.add_trace(go.Scatterpolar(
        r=current_values,
        theta=categories,
        fill='toself',
        name=f'{selected_month}',
        line=dict(color='rgb(0, 123, 255)', width=2),
        fillcolor='rgba(0, 123, 255, 0.3)'
    ))

    # 이전 월 트레이스 (있는 경우)
    if previous_summary:
        previous_values = [previous_summary[cat] for cat in categories]
        fig.add_trace(go.Scatterpolar(
            r=previous_values,
            theta=categories,
            fill='toself',
            name=f'{previous_month}',
            line=dict(color='rgb(255, 99, 71)', width=2),
            fillcolor='rgba(255, 99, 71, 0.2)'
        ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100]
            )
        ),
        showlegend=True,
        title=f"{selected_zone} 월별 지표 비교",
        height=500
    )

    st.plotly_chart(fig, use_container_width=True)

    # 상세 데이터 테이블
    st.markdown("---")
    st.subheader("📋 상세 데이터")

    detail_data = {
        '지표': categories + ['종합지표'],
        selected_month: [current_summary[cat] for cat in categories] + [current_summary['종합지표']]
    }

    if previous_summary:
        detail_data[previous_month] = [previous_summary[cat] for cat in categories] + [previous_summary['종합지표']]
        detail_data['변화량'] = [
            f"{current_summary[cat] - previous_summary[cat]:+.1f}%"
            for cat in categories
        ] + [f"{current_summary['종합지표'] - previous_summary['종합지표']:+.1f}%"]

    detail_df = pd.DataFrame(detail_data)

    # 퍼센트 기호 추가
    for col in detail_df.columns:
        if col not in ['지표', '변화량']:
            detail_df[col] = detail_df[col].apply(lambda x: f"{x}%")

    st.dataframe(detail_df, use_container_width=True, hide_index=True)

# ==================== 메인 앱 ====================
def main():
    # Google Sheets 연결
    client = connect_to_gsheet()

    if client is None:
        st.error("Google Sheets에 연결할 수 없습니다.")
        return

    # 마스터 DB 로드
    master_df = load_master_db(client)

    # 사이드바
    tab_selection, weights = render_sidebar(client, master_df)

    # 선택된 탭에 따라 화면 렌더링
    if tab_selection == "📊 지표 보기":
        render_scoreboard_tab(client, master_df, weights)
    elif tab_selection == "🎯 구역별 지표보기":
        render_zone_detail_tab(client, master_df, weights)
    elif tab_selection == "👥 회원 관리":
        render_member_management_tab(client, master_df)
    elif tab_selection == "📅 월 생성":
        render_monthly_generation_tab(client, master_df)
    elif tab_selection == "✏️ 데이터 입력":
        render_data_input_tab(client, master_df)

if __name__ == "__main__":
    main()
