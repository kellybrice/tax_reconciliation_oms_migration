with cte_f_orders as (
    select distinct order_number,
    sum(order_shipment_amt_usd) as shipping_cost_usd,
    sum(additional_tax_amt_usd) as tax_amount
    from elsa_oms_mvp.f_orders_union as orders
    left join elsa_oms_mvp.dim_store_union as stores
        on orders.store_code = stores.store_code
where date(convert_timezone(stores.store_region_timezone, orders.order_completed_at)) >= '2023-01-01'
and date(convert_timezone(stores.store_region_timezone, orders.order_completed_at)) < '2023-02-01'
and (NOT orders.is_free_order OR orders.is_free_order IS NULL)
and is_order_complete = true
group by 1
),
cte_f_order_items as (
    select distinct order_items.order_number,
    state_abbreviation,
    sum(distinct order_items.non_tax_price_amt_usd) as Gross_Revenue_without_tax,
    sum(distinct order_items.non_tax_price_amt_usd +  order_items.dist_promo_amt_usd) as Revenue_after_promotion_without_tax
    from elsa_oms_mvp.f_order_items_union as order_items
    --left join elsa.br_order_shipments as bridge
        --on order_items.order_id = bridge.order_id
    left join elsa.f_shipments as shipments
        on order_items.order_number = shipments.order_number
    left join elsa.dim_fulfillment_address as fulfill
        on shipments.fulfillment_address_id = fulfill.fulfillment_address_id
    left join elsa_oms_mvp.dim_store_union as stores
        on order_items.store_code = stores.store_code
    where date(convert_timezone(stores.store_region_timezone, order_items.order_completed_at)) >= '2023-01-01'
    and date(convert_timezone(stores.store_region_timezone, order_items.order_completed_at)) < '2023-02-01'
    and (NOT order_items.is_free_order OR order_items.is_free_order IS NULL)
    and order_items.order_item_status != 'canceled'
    group by 1,2
    )
    select distinct cte_f_order_items.order_number,
    state_abbreviation,
    Gross_Revenue_without_tax,
    Revenue_after_promotion_without_tax,
    shipping_cost_usd,
    tax_amount as tax_amount
    from cte_f_order_items
    left join cte_f_orders
        on cte_f_order_items.order_number = cte_f_orders.order_number;
