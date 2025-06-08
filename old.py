hybrid_search_df = None

with sqlite_connection() as conn:
    EMBEDDINGS_TITLE_WEIGHT = 0.7
    EMBEDDINGS_WEIGHT = 0.3
    TEXT_TITLE_WEIGHT = 3.0
    MAX_RESULTS = 20

    cursor = conn.cursor()

    cursor.execute("""
        WITH embeddings_field_similarities AS (
            SELECT 
                article_id AS id,
                field_name,
                (2 - vec_distance_cosine(embedding, :embedding)) / 2 AS similarity
            FROM article_embeddings
        ),
        embeddings_article_similarities AS (
            SELECT 
                id,
                MAX(CASE WHEN field_name = 'title' THEN similarity END) * :embeddings_title_weight AS title_weighed_similarity,
                (
                    MAX(CASE WHEN field_name = 'title' THEN similarity END) * :embeddings_title_weight +
                    MAX(CASE WHEN field_name = 'content' THEN similarity END) * (1 - :embeddings_title_weight)
                ) AS weighed_similarity
            FROM embeddings_field_similarities
            GROUP BY id
            HAVING 
                MAX(CASE WHEN field_name = 'title' THEN similarity END) IS NOT NULL AND
                MAX(CASE WHEN field_name = 'content' THEN similarity END) IS NOT NULL
        ),
        embeddings_max_similarity AS (
            SELECT
                MAX(weighed_similarity) AS max_weighed_similarity
            FROM embeddings_article_similarities
        ),
        embeddings_scores AS (
            SELECT
                eas.id AS id,
                (eas.weighed_similarity / ms.max_weighed_similarity) * :embeddings_weight AS score,
                eas.title_weighed_similarity / eas.weighed_similarity AS title_contribution
            FROM embeddings_article_similarities eas
            LEFT JOIN embeddings_max_similarity ms
        ),
        text_base_scores_title AS (
            SELECT 
                rowid AS id,
                rank * :text_title_weight AS score
            FROM articles_fts 
            WHERE articles_fts
            MATCH ('title:' || :query)
        ),
        text_base_scores_content AS (
            SELECT 
                rowid AS id,
                rank AS score
            FROM articles_fts 
            WHERE articles_fts
            MATCH ('content:' || :query)
        ),
        text_base_scores AS (
            SELECT
                a.id AS id,
                COALESCE(tbst.score, 0) + COALESCE(tbsc.score, 0) AS score
            FROM articles a
            LEFT JOIN text_base_scores_title tbst ON a.id = tbst.id
            LEFT JOIN text_base_scores_content tbsc ON a.id = tbsc.id
            WHERE COALESCE(tbst.id, tbsc.id) IS NOT NULL
        ),
        text_max_base_score AS (
            SELECT
                MAX(score) AS max_score
            FROM text_base_scores
        ),
        text_scores AS (
            SELECT
                tbs.id AS id,
                (tbs.score / tmbs.max_score) * (1 - :embeddings_weight) AS score
            FROM text_base_scores tbs
            LEFT JOIN text_max_base_score tmbs
        ),
        concat_scores AS (
            SELECT
                a.id AS id,
                COALESCE(es.title_contribution, 0) AS embeddings_title_contribution,
                COALESCE(es.score, 0) AS embeddings_score,
                COALESCE(ts.score, 0) AS text_score,
                COALESCE(es.score, 0) + COALESCE(ts.score, 0) AS score
            FROM articles a
            LEFT JOIN embeddings_scores es ON a.id = es.id
            LEFT JOIN text_scores ts ON a.id = ts.id
        ),
        concat_max_score AS (
            SELECT
                MAX(score) AS max_score
            FROM concat_scores
        ),
        hybrid_scores AS (
            SELECT
                cs.id AS id,
                (cs.embeddings_title_contribution * cs.embeddings_score) / cs.score AS embeddings_title_contribution,
                ((1 - cs.embeddings_title_contribution) * cs.embeddings_score) / cs.score AS embeddings_content_contribution,
                cs.text_score / cs.score AS text_contribution,
                (cs.score / cms.max_score) AS score
            FROM concat_scores cs
            LEFT JOIN concat_max_score cms
        )
        SELECT
            a.id AS id,
            a.title AS title,
            a.section AS section,
            a.link AS link,
            hs.score AS score,
            PRINTF("%.1f%%", hs.embeddings_title_contribution * 100) AS embeddings_title_contribution_percent,
            PRINTF("%.1f%%", hs.embeddings_content_contribution * 100) AS embeddings_content_contribution_percent,
            PRINTF("%.1f%%", hs.text_contribution * 100) AS text_contribution_percent
        FROM articles a
        LEFT JOIN hybrid_scores hs ON a.id = hs.id
        ORDER BY score DESC
        LIMIT :max_results
    """, {
        'query': QUERY,
        'max_results': MAX_RESULTS,
        'embedding': sqlite_vec.serialize_float32(query_embedding),
        'embeddings_title_weight': EMBEDDINGS_TITLE_WEIGHT,
        'embeddings_weight': EMBEDDINGS_WEIGHT,
        'text_title_weight': TEXT_TITLE_WEIGHT
    })

    hybrid_search_df = pd.DataFrame(cursor, columns=["Id", "Tytuł", "Sekcja", "Link", "Wynik (0-1)", "Udział osadzenia tytułu", "Udział osadzenia zawartości", "Udział wyszukiwania tekstowego"])

hybrid_search_df