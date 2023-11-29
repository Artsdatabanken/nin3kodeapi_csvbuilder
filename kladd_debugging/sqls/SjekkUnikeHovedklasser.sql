Select 'Type' as klasse , count(distinct(kode)) as unique_count, count(*) as count  from Type
UNION
Select 'Hovedtypegruppe' as klasse , count(distinct(kode)) as unique_count, count(*) as count  from Hovedtypegruppe
UNION
Select 'Hovedtype' as klasse , count(distinct(kode)) as unique_count, count(*) as count  from Hovedtype
UNION
Select 'Grunntype' as klasse , count(distinct(kode)) as unique_count, count(*) as count  from Grunntype
UNION
Select 'Variabel' as klasse , count(distinct(kode)) as unique_count, count(*) as count  from Variabel
UNION
Select 'Variabelnavn' as klasse , count(distinct(kode)) as unique_count, count(*) as count  from Variabelnavn