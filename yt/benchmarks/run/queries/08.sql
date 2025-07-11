-- NB: Subqueries
$bla1 = (      SELECT substring(cast(ca_zip as string),0,5) AS ca_zip
      FROM customer_address
      WHERE substring(cast(ca_zip as string),0,5) IN (
                          '47602','16704','35863','28577','83910','36201',
                          '58412','48162','28055','41419','80332',
                          '38607','77817','24891','16226','18410',
                          '21231','59345','13918','51089','20317',
                          '17167','54585','67881','78366','47770',
                          '18360','51717','73108','14440','21800',
                          '89338','45859','65501','34948','25973',
                          '73219','25333','17291','10374','18829',
                          '60736','82620','41351','52094','19326',
                          '25214','54207','40936','21814','79077',
                          '25178','75742','77454','30621','89193',
                          '27369','41232','48567','83041','71948',
                          '37119','68341','14073','16891','62878',
                          '49130','19833','24286','27700','40979',
                          '50412','81504','94835','84844','71954',
                          '39503','57649','18434','24987','12350',
                          '86379','27413','44529','98569','16515',
                          '27287','24255','21094','16005','56436',
                          '91110','68293','56455','54558','10298',
                          '83647','32754','27052','51766','19444',
                          '13869','45645','94791','57631','20712',
                          '37788','41807','46507','21727','71836',
                          '81070','50632','88086','63991','20244',
                          '31655','51782','29818','63792','68605',
                          '94898','36430','57025','20601','82080',
                          '33869','22728','35834','29086','92645',
                          '98584','98072','11652','78093','57553',
                          '43830','71144','53565','18700','90209',
                          '71256','38353','54364','28571','96560',
                          '57839','56355','50679','45266','84680',
                          '34306','34972','48530','30106','15371',
                          '92380','84247','92292','68852','13338',
                          '34594','82602','70073','98069','85066',
                          '47289','11686','98862','26217','47529',
                          '63294','51793','35926','24227','14196',
                          '24594','32489','99060','49472','43432',
                          '49211','14312','88137','47369','56877',
                          '20534','81755','15794','12318','21060',
                          '73134','41255','63073','81003','73873',
                          '66057','51184','51195','45676','92696',
                          '70450','90669','98338','25264','38919',
                          '59226','58581','60298','17895','19489',
                          '52301','80846','95464','68770','51634',
                          '19988','18367','18421','11618','67975',
                          '25494','41352','95430','15734','62585',
                          '97173','33773','10425','75675','53535',
                          '17879','41967','12197','67998','79658',
                          '59130','72592','14851','43933','68101',
                          '50636','25717','71286','24660','58058',
                          '72991','95042','15543','33122','69280',
                          '11912','59386','27642','65177','17672',
                          '33467','64592','36335','54010','18767',
                          '63193','42361','49254','33113','33159',
                          '36479','59080','11855','81963','31016',
                          '49140','29392','41836','32958','53163',
                          '13844','73146','23952','65148','93498',
                          '14530','46131','58454','13376','13378',
                          '83986','12320','17193','59852','46081',
                          '98533','52389','13086','68843','31013',
                          '13261','60560','13443','45533','83583',
                          '11489','58218','19753','22911','25115',
                          '86709','27156','32669','13123','51933',
                          '39214','41331','66943','14155','69998',
                          '49101','70070','35076','14242','73021',
                          '59494','15782','29752','37914','74686',
                          '83086','34473','15751','81084','49230',
                          '91894','60624','17819','28810','63180',
                          '56224','39459','55233','75752','43639',
                          '55349','86057','62361','50788','31830',
                          '58062','18218','85761','60083','45484',
                          '21204','90229','70041','41162','35390',
                          '16364','39500','68908','26689','52868',
                          '81335','40146','11340','61527','61794',
                          '71997','30415','59004','29450','58117',
                          '69952','33562','83833','27385','61860',
                          '96435','48333','23065','32961','84919',
                          '61997','99132','22815','56600','68730',
                          '48017','95694','32919','88217','27116',
                          '28239','58032','18884','16791','21343',
                          '97462','18569','75660','15475'));
$bla2 = (select ca_zip
      from (SELECT substring(cast(customer_address.ca_zip as string),0,5) ca_zip,count(*) cnt
            FROM customer_address cross join customer
            WHERE ca_address_sk = c_current_addr_sk and
                  c_preferred_cust_flag='Y'
            group by customer_address.ca_zip
            having count(*) > 10)A1);

-- start query 1 in stream 0 using template query8.tpl and seed 1766988859
select  store.s_store_name
      ,sum(ss_net_profit)
 from store_sales
     cross join date_dim
     cross join store cross join
     (select ca_zip
     from (
      select
        bla1.ca_zip as ca_zip
      from any $bla1 bla1
      join any $bla2 bla2 on StablePickle(bla1.ca_zip) == StablePickle(bla2.ca_zip)
     ) A2) V1
 where ss_store_sk = s_store_sk
  and ss_sold_date_sk = d_date_sk
  and d_qoy = 2 and d_year = 1998
  and (substring(cast(s_zip as string),0,2) = substring(V1.ca_zip,0,2))
 group by store.s_store_name
 order by store.s_store_name
 limit 100;

-- end query 1 in stream 0 using template query8.tpl
