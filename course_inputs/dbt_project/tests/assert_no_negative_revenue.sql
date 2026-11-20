-- בדיקה "סינגולרית": מחזירה שורות בעייתיות. 0 שורות = עבר.
select * from {{ ref('mart_daily_revenue') }} where revenue < 0
