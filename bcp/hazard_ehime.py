EHIME_KEYWORDS = [
    "愛媛県",
    "松山市",
    "今治市",
    "宇和島市",
    "八幡浜市",
    "新居浜市",
    "西条市",
    "大洲市",
    "伊予市",
    "四国中央市",
    "西予市",
    "東温市",
    "上島町",
    "久万高原町",
    "松前町",
    "砥部町",
    "内子町",
    "伊方町",
    "松野町",
    "鬼北町",
    "愛南町",
]

EHIME_COASTAL_MUNICIPALITIES = [
    "松山市",
    "今治市",
    "宇和島市",
    "八幡浜市",
    "新居浜市",
    "西条市",
    "大洲市",
    "伊予市",
    "四国中央市",
    "西予市",
    "上島町",
    "松前町",
    "伊方町",
    "愛南町",
]

COASTAL_AREA_KEYWORDS = [
    "海岸",
    "港",
    "湾",
    "埠頭",
    "三津",
    "高浜",
    "堀江",
    "北条",
    "空港",
    "長浜",
    "八幡浜港",
    "三崎",
    "波止浜",
    "壬生川",
]


HIGH_RISK_INDUSTRIES = [
    "A 農業・林業",
    "B 漁業",
    "D 建設業",
    "E 製造業",
    "F 電気・ガス・熱供給・水道業",
    "H 運輸業・郵便業",
    "I 卸売業・小売業",
    "K 不動産業・物品賃貸業",
    "M 宿泊業・飲食サービス業",
    "P 医療・福祉",
    "Q 複合サービス事業",
]

MEDIUM_RISK_INDUSTRIES = [
    "C 鉱業・採石業・砂利採取業",
    "G 情報通信業",
    "J 金融業・保険業",
    "L 学術研究・専門技術サービス業",
    "N 生活関連サービス業・娯楽業",
    "O 教育・学習支援業",
    "R サービス業（他に分類されないもの）",
]

LOW_RISK_INDUSTRIES = [
    "S 公務（他に分類されるものを除く）",
    "T 分類不能の産業",
]


def is_ehime_address(address: str) -> bool:
    if not address:
        return False
    return any(keyword in address for keyword in EHIME_KEYWORDS)


def is_ehime_coastal(address: str) -> bool:
    if not address:
        return False

    if any(keyword in address for keyword in EHIME_COASTAL_MUNICIPALITIES):
        return True

    if any(keyword in address for keyword in COASTAL_AREA_KEYWORDS):
        return True

    return False


def get_industry_risk_level(industry: str) -> str:
    if industry in HIGH_RISK_INDUSTRIES:
        return "high"
    if industry in MEDIUM_RISK_INDUSTRIES:
        return "medium"
    if industry in LOW_RISK_INDUSTRIES:
        return "low"
    return "unknown"


def detect_nankai_mode(customer_row) -> bool:
    address = str(customer_row.get("address1", "")).strip()
    industry = str(customer_row.get("industry", "")).strip()
    priority = str(customer_row.get("nankai_priority", "")).strip()

    if priority == "高":
        return True

    if not address:
        return False

    if not is_ehime_address(address):
        return False

    if is_ehime_coastal(address):
        return True

    if get_industry_risk_level(industry) == "high" and priority == "中":
        return True

    return False


def estimate_shutdown_days_by_ehime_hazard(customer_row) -> int:
    address = str(customer_row.get("address1", "")).strip()
    industry = str(customer_row.get("industry", "")).strip()
    priority = str(customer_row.get("nankai_priority", "")).strip()

    risk_level = get_industry_risk_level(industry)

    if detect_nankai_mode(customer_row):
        return 180

    if not address:
        if priority == "中":
            return 90
        return 90

    if not is_ehime_address(address):
        return 90

    if risk_level == "high":
        return 90

    if risk_level == "medium":
        return 90

    return 90


def build_hazard_comment(customer_row) -> str:
    address = str(customer_row.get("address1", "")).strip()
    industry = str(customer_row.get("industry", "")).strip()
    priority = str(customer_row.get("nankai_priority", "")).strip()

    if not address:
        if priority == "高":
            return "住所未入力のため、南海トラフ優先度「高」をもとに180日基準で確認"
        return "住所未入力のため、標準90日基準で確認"

    if not is_ehime_address(address):
        return "愛媛県外住所のため、標準90日基準で確認"

    if detect_nankai_mode(customer_row):
        return "愛媛沿岸または南海トラフ優先度を考慮し、180日基準で確認"

    risk_level = get_industry_risk_level(industry)
    if risk_level == "high":
        return "愛媛県内の高リスク業種として、標準90日基準で確認"

    if risk_level == "medium":
        return "愛媛県内の中リスク業種として、標準90日基準で確認"

    if risk_level == "low":
        return "愛媛県内の低リスク業種として、標準90日基準で確認"

    return "愛媛県内の標準ルールとして90日基準で確認"