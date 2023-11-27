select gt.kode as GTkode, vn.Kode as VNkode, ms.MaaleskalaNavn,t.Verdi from GrunntypeVariabeltrinn gtvt, 
              Variabelnavn vn,
              Grunntype gt,
              Maaleskala ms,
              Trinn t
where gtvt.VariabelnavnId = vn.Id 
      and gtvt.GrunntypeId = gt.Id
      and gtvt.MaaleskalaId = ms.Id
      and gtvt.TrinnId = t.Id