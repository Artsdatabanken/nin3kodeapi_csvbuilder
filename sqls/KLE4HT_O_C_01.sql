Select Id from Hovedtype where kode = 'O-C-01'; -- 310

--Select * from Kartleggingsenhet where id IN (select KartleggingsenhetId from Hovedtype_Kartleggingsenhet where HovedtypeId = 310);