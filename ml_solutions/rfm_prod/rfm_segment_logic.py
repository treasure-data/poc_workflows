    if r == 4 and f == 4 and m == 4:
        return "Champions"
    elif r >= 3:
        if f >= 3 and m >= 3:
            return "Loyal Customers"
        if f >= 2 and m >= 2:
            return "Potential Loyalists"
        if f >= 2 or m >= 2:
            return "Promising"
        else:
            assert f == 1, f"Unexpected f value: {f}"
            assert m == 1, f"Unexpected m value: {m}"
            return "New Customers"
    elif r == 2:
        if m >= 3:
            return "Cannot lose them"  # enough revenue comes for this segment and better not lose their attention
        elif m == 2 and f >= 2:
            return "Need attention"  # potential loyalist is losing attention
        else:
            return "Hibernating"  # not worth give attention to them
    else:
        assert r == 1, f"Unexpected r value: {r}"
        if m >= 3 or (m == 2 and f >= 2):
            return "High Value Sleeping"  # past potential loyalist sleeping
        else:
            return "Lost customers"