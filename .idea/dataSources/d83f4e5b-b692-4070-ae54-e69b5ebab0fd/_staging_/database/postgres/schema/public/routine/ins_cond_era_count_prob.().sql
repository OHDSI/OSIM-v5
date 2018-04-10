create or REPLACE function ins_cond_era_count_prob() returns void
LANGUAGE plpgsql
AS $$
DECLARE
    num_rows INTEGER;
    MESSAGE              VARCHAR(500);
  BEGIN
    PERFORM insert_log('Starting Condition Era Count Analysis', 'ins_cond_era_count_prob');
    PERFORM 'TRUNCATE TABLE osim_cond_era_count_prob';


    -- Drop Indexes for Quicker Insertion
    BEGIN
      PERFORM 'DROP INDEX osim_cond_era_count_ix1';
      PERFORM 'DROP INDEX osim_cond_era_count_ix2';
    EXCEPTION
      WHEN OTHERS THEN
        PERFORM insert_log('Probability indexes are already removed',
            'ins_cond_era_count_prob');
    END;



    INSERT /*+ append nologging */ INTO osim_cond_era_count_prob
     (condition_concept_id, cond_count_bucket, time_remaining, cond_era_count,
      n, accumulated_probability)
    SELECT
      condition_concept_id,
      conds_bucket,
      time_remaining,
      eras,
      persons AS n,
      SUM(probability) OVER
       (PARTITION BY condition_concept_id, conds_bucket, time_remaining
        ORDER BY probability DESC ROWS UNBOUNDED PRECEDING) accumulated_probability
    FROM
     (SELECT
        condition_concept_id,
        conds_bucket,
        time_remaining,
        COUNT(person_id) as persons,
        eras,
        1.0 * COUNT(person_id)/ NULLIF(SUM(COUNT(person_id)) OVER(PARTITION BY condition_concept_id, conds_bucket, time_remaining), 0) AS probability
      FROM
       (SELECT
          cond.condition_concept_id,
          osim__condition_count_bucket(person.condition_concepts) AS conds_bucket,
          osim__time_observed_bucket(person.observation_period_end_date
            - MIN(cond.condition_era_start_date)) AS time_remaining,
          COUNT(DISTINCT cond.condition_era_id) AS eras,
          person.person_id
        FROM v_src_person_strata person
        INNER JOIN v_src_condition_era1 cond ON person.person_id = cond.person_id
        GROUP BY
          cond.condition_concept_id,
          person.condition_concepts,
          person.observation_period_end_date,
          person.person_id) t1
      GROUP BY condition_concept_id, eras, conds_bucket, time_remaining) t2
    ORDER BY 1,2,3,6;

    GET DIAGNOSTICS num_rows = ROW_COUNT;
    MESSAGE := num_rows || ' rows inserted into osim_cond_era_count_prob.';
    PERFORM insert_log(MESSAGE, 'ins_cond_era_count_prob');



    PERFORM '
      CREATE INDEX osim_cond_era_count_ix1 ON osim_cond_era_count_prob (
        condition_concept_id,
        cond_count_bucket,
        time_remaining)
      PCTFREE 10 INITRANS 2 MAXTRANS 255 NOLOGGING
      STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
      PCTINCREASE 0 FREELISTS 5 FREELIST GROUPS 5 BUFFER_POOL DEFAULT)';

    PERFORM '
      CREATE INDEX osim_cond_era_count_ix2 ON osim_cond_era_count_prob (
        accumulated_probability)
      PCTFREE 10 INITRANS 2 MAXTRANS 255 NOLOGGING
      STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
      PCTINCREASE 0 FREELISTS 5 FREELIST GROUPS 5 BUFFER_POOL DEFAULT)';



    UPDATE osim_cond_era_count_prob
    SET accumulated_probability = 1.0
    WHERE oid IN
     (SELECT DISTINCT
        FIRST_VALUE(oid)
          OVER
           (PARTITION BY condition_concept_id, cond_count_bucket, time_remaining
            ORDER BY accumulated_probability DESC)
      FROM osim_cond_era_count_prob);



    PERFORM insert_log('Processing complete', 'ins_cond_era_count_prob');

    EXCEPTION
    WHEN OTHERS THEN
    PERFORM insert_log('Exception', 'ins_cond_era_count_prob');
    raise notice '% %', SQLERRM, SQLSTATE;

  END;
$$;
