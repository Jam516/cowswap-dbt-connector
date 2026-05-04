{%- set partner_start_date = var('partner_start_date', '2026-04-01') -%}
{%- set time_start = "date '" ~ partner_start_date ~ "'" -%}
{%- set time_start_incremental = "greatest(" ~ time_start ~ ", current_date - interval '7' day)" -%}
{%- set time_end = "current_date + interval '1' day" -%}
{%- set chains = [
    {'blockchain': 'base', 'source_name': 'cow_protocol_base', 'raw_orders_table': 'result_cow_base_raw_orders'},
    {'blockchain': 'ethereum', 'source_name': 'cow_protocol_ethereum', 'raw_orders_table': 'result_cow_ethereum_raw_orders'},
    {'blockchain': 'arbitrum', 'source_name': 'cow_protocol_arbitrum', 'raw_orders_table': 'result_cow_arbitrum_raw_orders'},
    {'blockchain': 'polygon', 'source_name': 'cow_protocol_polygon', 'raw_orders_table': 'result_cow_polygon_raw_orders'},
    {'blockchain': 'bnb', 'source_name': 'cow_protocol_bnb', 'raw_orders_table': 'result_cow_bnb_raw_orders'},
    {'blockchain': 'avalanche_c', 'source_name': 'cow_protocol_avalanche_c', 'raw_orders_table': 'result_cow_avalanche_c_raw_orders'}
] -%}

{{ config(
    alias = 'dune_cowswap_trades'
    , materialized = 'incremental'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'blockchain', 'order_uid', 'tx_hash']
    , incremental_predicates = ["DBT_INTERNAL_DEST.block_date >= " ~ time_start_incremental]
    , meta = {
        "dune": {
            "public": false
        },
        "datashare": {
            "enabled": true,
            "time_column": "block_date",
            "time_start": time_start,
            "time_start_incremental": time_start_incremental,
            "time_end": time_end
        }
    }
    , properties = {
        "partitioned_by": "ARRAY['block_date']"
    }
) }}

with partner_info as (
    -- Override these dbt vars per partner run:
    -- partner_cut, partner_fee_recipient_address, partner_start_date.
    select
        {{ var('partner_cut', 0.75) }} as partner_cut
        , {{ var('partner_fee_recipient_address', '0x000000') }} as partner_fee_recipient_address
        , timestamp '{{ partner_start_date }}' as partner_start_date
)
, prep as (
    {% for chain in chains %}
    select
        date(t.block_time) as block_date
        , t.block_time
        , '{{ chain.blockchain }}' as blockchain
        , t.order_uid
        , t.tx_hash
        , t.trader
        , t.usd_value
        , t.surplus_usd
        , t.order_type
        , t.partial_fill
        , t.sell_token as sell_token_symbol
        , t.sell_token_address
        , t.units_sold
        , t.buy_token as buy_token_symbol
        , t.buy_token_address
        , t.units_bought
        , partner_info.partner_cut * coalesce(rod.partner_fee, 0) * rod.protocol_fee_native_price / 1e18 as partner_fee_partner_cut_native
        , rod.protocol_fee_token as fee_token_address
        , rod.protocol_fee_native_price as fee_token_native_price
        , t.receiver
        , t.app_data
    from {{ source(chain.source_name, 'trades') }} as t
    left join (
        select distinct
            order_uid
            , tx_hash
            , partner_fee
            , protocol_fee_token
            , protocol_fee_native_price
            , partner_fee_recipient
        from {{ source('coinbase_shared', chain.raw_orders_table, database='dune') }}
    ) as rod
        on rod.order_uid = t.order_uid
        and rod.tx_hash = t.tx_hash
    cross join partner_info
    where
        t.block_time >= greatest(
            partner_info.partner_start_date
            , cast({{ time_start_incremental if is_incremental() else time_start }} as timestamp)
        )
        and t.block_time < cast({{ time_end }} as timestamp)
        and partner_info.partner_fee_recipient_address = rod.partner_fee_recipient
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)
, daily_native_token_prices as (
    select
        date(p.timestamp) as block_date
        , p.blockchain
        , p.price
    from {{ source('prices', 'day') }} as p
    inner join {{ source('dune', 'blockchains') }} as b
        on p.blockchain = b.name
        and p.contract_address = b.token_address
    where
        p.timestamp >= cast({{ time_start_incremental if is_incremental() else time_start }} as timestamp)
        and p.timestamp < cast({{ time_end }} as timestamp)
        and p.blockchain in (
            {% for chain in chains %}
            '{{ chain.blockchain }}'{% if not loop.last %}, {% endif %}
            {% endfor %}
        )
)
, fees_with_conversions as (
    select
        prep.*
        , prep.partner_fee_partner_cut_native * p_native.price as partner_fee_partner_cut_usd
    from prep
    left join daily_native_token_prices as p_native
        on prep.block_date = p_native.block_date
        and prep.blockchain = p_native.blockchain
)
select
    block_date
    , block_time
    , case
        when blockchain = 'bnb' then 'bsc'
        when blockchain = 'avalanche' then 'avalanche_c'
        else blockchain
    end as blockchain
    , order_uid
    , tx_hash
    , trader
    , usd_value
    , surplus_usd
    , order_type
    , partial_fill
    , sell_token_symbol
    , sell_token_address
    , units_sold
    , buy_token_symbol
    , buy_token_address
    , units_bought
    , partner_fee_partner_cut_native
    , fee_token_address
    , fee_token_native_price
    , receiver
    , app_data
    , partner_fee_partner_cut_usd
from fees_with_conversions
