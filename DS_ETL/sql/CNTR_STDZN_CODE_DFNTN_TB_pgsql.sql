CREATE TABLE CNTR_STDZN_CODE_DFNTN_TB(
                                 CNTR_ID char(2) not null,
                                 CNTR_NM varchar(30),
                                 CD_NM varchar(50),
                                 CD_VL varchar(50) not null,
                                 CD_KOR_MNNG varchar(200),
                                 CD_ENG_MNNG varchar(200),
                                 CD_EXPLN varchar(200),
                                 CREATE_TIME varchar(14),
                                 UPDATE_TIME varchar(14),
								 PRIMARY KEY(CNTR_ID, CD_NM, CD_VL, CREATE_TIME)
								 )
