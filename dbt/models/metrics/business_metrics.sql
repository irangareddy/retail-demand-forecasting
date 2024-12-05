WITH inventory_metrics AS (
    SELECT 
        month,
        state,
        category_code,
        
        -- Inventory carrying costs
        inventory_value,
        storage_cost,
        insurance_cost,
        obsolescence_cost,
        total_carrying_cost,
        
        -- Stockout events
        stockout_events,
        total_demand,
        lost_sales_value,
        
        -- Resource allocation
        labor_hours_planned,
        labor_hours_actual,
        
        -- Decision timing
        DATEDIFF('minutes', decision_request_time, decision_made_time) as decision_time
    FROM {{ ref('stg_business_performance') }}
),

baseline_metrics AS (
    SELECT
        AVG(total_carrying_cost) as baseline_carrying_cost,
        COUNT(stockout_events) as baseline_stockouts,
        AVG(labor_hours_planned / NULLIF(labor_hours_actual, 0)) as baseline_resource_efficiency,
        AVG(decision_time) as baseline_decision_time
    FROM inventory_metrics
    WHERE month < (SELECT MIN(month) FROM {{ ref('stg_forecast_performance') }})
),

current_metrics AS (
    SELECT
        AVG(total_carrying_cost) as current_carrying_cost,
        COUNT(stockout_events) as current_stockouts,
        AVG(labor_hours_planned / NULLIF(labor_hours_actual, 0)) as current_resource_efficiency,
        AVG(decision_time) as current_decision_time
    FROM inventory_metrics
    WHERE month >= (SELECT MIN(month) FROM {{ ref('stg_forecast_performance') }})
)

SELECT 
    -- Inventory cost reduction
    ((baseline_carrying_cost - current_carrying_cost) / NULLIF(baseline_carrying_cost, 0)) * 100 
        as carrying_cost_reduction_percent,
    
    -- Stockout reduction
    ((baseline_stockouts - current_stockouts) / NULLIF(baseline_stockouts, 0)) * 100 
        as stockout_reduction_percent,
    
    -- Resource allocation improvement
    ((current_resource_efficiency - baseline_resource_efficiency) / NULLIF(baseline_resource_efficiency, 0)) * 100 
        as resource_efficiency_improvement_percent,
    
    -- Decision time reduction
    ((baseline_decision_time - current_decision_time) / NULLIF(baseline_decision_time, 0)) * 100 
        as decision_time_reduction_percent,
        
    -- Success criteria evaluation
    CASE 
        WHEN ((baseline_carrying_cost - current_carrying_cost) / NULLIF(baseline_carrying_cost, 0)) * 100 >= 20 
        THEN 'Meeting Target' 
        ELSE 'Needs Improvement'
    END as carrying_cost_status,
    
    CASE 
        WHEN ((baseline_stockouts - current_stockouts) / NULLIF(baseline_stockouts, 0)) * 100 >= 50 
        THEN 'Meeting Target' 
        ELSE 'Needs Improvement'
    END as stockout_reduction_status,
    
    CASE 
        WHEN ((current_resource_efficiency - baseline_resource_efficiency) / NULLIF(baseline_resource_efficiency, 0)) * 100 >= 15 
        THEN 'Meeting Target' 
        ELSE 'Needs Improvement'
    END as resource_efficiency_status,
    
    CASE 
        WHEN ((baseline_decision_time - current_decision_time) / NULLIF(baseline_decision_time, 0)) * 100 >= 40 
        THEN 'Meeting Target' 
        ELSE 'Needs Improvement'
    END as decision_time_status

FROM baseline_metrics
CROSS JOIN current_metrics