{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='REPLICATE'    
    )
}}

        
SELECT * 
FROM (
  SELECT
    cast(cik as BIGINT) companyid,
    st.st_name status,
    companyname name,
    ind.in_name industry,
    CASE 
        WHEN SPrating IN ('AAA','AA','AA+','AA-','A','A+','A-','BBB','BBB+','BBB-','BB','BB+','BB-','B','B+','B-','CCC','CCC+','CCC-','CC','C','D')
        THEN SPrating
        ELSE cast(null as varchar(4))
    END sprating,
    CASE
      WHEN SPrating IN ('AAA','AA','A','AA+','A+','AA-','A-','BBB','BBB+','BBB-') THEN 0
      WHEN SPrating IN ('BB','B','CCC','CC','C','D','BB+','B+','CCC+','BB-','B-','CCC-') THEN 1
      ELSE cast(null as bit)
    END as islowgrade, 
    ceoname ceo,
    addrline1 addressline1,
    addrline2 addressline2,
    postalcode,
    city,
    stateprovince stateprov,
    country,
    description,
    foundingdate,
    CASE WHEN lead(pts) OVER (PARTITION BY cik ORDER BY pts) IS NOT NULL THEN 1 ELSE 0 END iscurrent,
    1 batchid,
    cast(pts as date) effectivedate,
    concat(cast(cik as BIGINT), '-', cast(pts as date)) sk_companyid,
    coalesce(
      lead(cast(pts as date)) OVER (PARTITION BY cik ORDER BY pts),
      cast('9999-12-31' as date)) enddate
  FROM (
    SELECT
      convert(datetime2, substring([value],1,8)+' '+substring([value],10,2)+':'+substring([value],12,2)+':'+substring([value],14,2), 112) AS PTS,
      trim(substring(value, 19, 60)) AS CompanyName,
      trim(substring(value, 79, 10)) AS CIK,
      trim(substring(value, 89, 4)) AS Status,
      trim(substring(value, 93, 2)) AS IndustryID,
      trim(substring(value, 95, 4)) AS SPrating,
      convert(date, CASE WHEN trim(substring(value, 99, 8))='' THEN NULL ELSE substring(value, 99, 8) END, 112) AS FoundingDate,
      trim(substring(value, 107, 80)) AS AddrLine1,
      trim(substring(value, 187, 80)) AS AddrLine2,
      trim(substring(value, 267, 12)) AS PostalCode,
      trim(substring(value, 279, 25)) AS City,
      trim(substring(value, 304, 20)) AS StateProvince,
      trim(substring(value, 324, 24)) AS Country,
      trim(substring(value, 348, 46)) AS CEOname,
      trim(substring(value, 394, 150)) AS Description
    FROM {{ ref('FinWire_CMP') }}
       ) cmp
  JOIN {{ ref('StatusType') }} st ON cmp.status = st.st_id
  JOIN {{ ref('Industry') }} ind ON cmp.industryid = ind.in_id
) T
