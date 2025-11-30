"""
샘플 데이터 생성 및 CSV 저장 스크립트
UTF-8 BOM 인코딩으로 저장하여 Excel과 Google Sheets에서 한글이 깨지지 않도록 함
"""

import pandas as pd
import random
from datetime import datetime

# 샘플 데이터 생성
def generate_sample_data():
    """청년회 샘플 데이터 108개 레코드 생성"""

    # 한국 이름 목록
    last_names = ['김', '이', '박', '정', '최', '강', '조', '윤', '장', '임',
                  '한', '오', '신', '권', '송', '유', '홍', '배', '노', '문',
                  '서', '표', '남', '곽', '성', '황', '안', '전', '방', '주',
                  '태', '탁', '석', '추', '변', '도']

    first_names_male = ['민준', '지호', '예준', '도윤', '시우', '수빈', '하준',
                        '정민', '지훈', '승현', '주원', '시현', '민재', '도현',
                        '민규', '우진', '준서', '연우']

    first_names_female = ['서윤', '수아', '하은', '서준', '지우', '민서', '예은',
                          '지안', '채원', '서현', '다은', '은우', '서진', '예린',
                          '지율', '서영', '아인', '유나']

    # 지역 및 구역 설정
    regions = {
        '도원': ['1구역', '2구역', '3구역'],
        '송파': ['4구역', '5구역', '6구역'],
        '강동': ['7구역', '8구역', '9구역']
    }

    # 청년 명단 생성 (구역별 4명씩, 총 36명)
    people = []
    for region, zones in regions.items():
        for zone in zones:
            # 리더 1명
            leader_name = random.choice(last_names) + random.choice(first_names_male + first_names_female)
            people.append({
                'name': leader_name,
                'region': region,
                'zone': zone,
                'position': '리더'
            })

            # 청년 3명
            for _ in range(3):
                youth_name = random.choice(last_names) + random.choice(first_names_male + first_names_female)
                people.append({
                    'name': youth_name,
                    'region': region,
                    'zone': zone,
                    'position': '청년'
                })

    # 월별 데이터 생성 (날짜 형식: "11월", "12월")
    dates = ['9월', '10월', '11월']

    records = []

    for date in dates:
        for person in people:
            # 지표별로 랜덤하게 0 또는 1 할당 (현실적인 분포)
            # 리더는 더 높은 이행률
            if person['position'] == '리더':
                attendance_prob = 0.95
                participation_prob = 0.9
            else:
                attendance_prob = 0.75
                participation_prob = 0.7

            record = {
                '날짜': date,
                '이름': person['name'],
                '지역': person['region'],
                '구역': person['zone'],
                '직분': person['position'],
                '상태': '재적',
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

# 데이터 생성
print("샘플 데이터 생성 중...")
df = generate_sample_data()

# UTF-8 BOM으로 CSV 저장
print("CSV 파일 저장 중 (UTF-8 BOM 인코딩)...")
df.to_csv('sample_data.csv', index=False, encoding='utf-8-sig')

print(f"✅ 완료! {len(df)}개의 레코드가 생성되었습니다.")
print(f"   파일: sample_data.csv")
print(f"\n데이터 미리보기:")
print(df.head(10))
print(f"\n지역별 인원 수:")
print(df[df['날짜'] == '11월'].groupby('지역')['이름'].count())
