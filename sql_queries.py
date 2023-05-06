query_df_exited_age_correlation = '''
SELECT
        geography,
        gender,
        exited,
        AVG(age) AS avg_age,
        ROUND(AVG(estimatedsalary),1) AS avg_salary,
        COUNT(*) as number_of_exited_or_not
FROM df
GROUP BY geography, gender, exited
ORDER BY COUNT(*)
'''

query_df_exited_salary_correlation = '''
SELECT
        exited,
        is_greater,
        CASE
        WHEN exited=is_greater THEN 1
        ELSE 0
        END AS correlation
FROM df
'''