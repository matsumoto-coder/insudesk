def calculate_available_funds(
    cash_on_hand: int,
    credit_line: int,
    estimated_insurance: int,
) -> int:
    return (
        max(cash_on_hand, 0)
        + max(credit_line, 0)
        + max(estimated_insurance, 0)
    )


def calculate_survival_days(
    monthly_fixed_cost: int,
    available_funds: int,
) -> int:
    if monthly_fixed_cost <= 0:
        return 0
    return int((available_funds / monthly_fixed_cost) * 30)


def calculate_power_percent(
    survival_days: int,
    ideal_days: int = 90,
    max_percent: int = 200,
) -> int:
    if ideal_days <= 0:
        return 0

    percent = int((survival_days / ideal_days) * 100)
    return min(max(percent, 0), max_percent)


def calculate_funding_gap(
    monthly_fixed_cost: int,
    shutdown_days: int,
    repair_cost: int = 0,
    restocking_cost: int = 0,
    available_funds: int = 0,
) -> int:
    if monthly_fixed_cost < 0:
        monthly_fixed_cost = 0

    shutdown_months = shutdown_days / 30

    required_funds = (
        int(monthly_fixed_cost * shutdown_months)
        + max(repair_cost, 0)
        + max(restocking_cost, 0)
    )

    return required_funds - max(available_funds, 0)