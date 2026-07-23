WITH dedup AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_answer_timestamp DESC, review_creation_date DESC) as rn
    FROM {{ source('bronze', 'reviews') }}
)
SELECT 
    review_id, 
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
FROM dedup
WHERE
    rn = 1 AND
    review_id IS NOT NULL


