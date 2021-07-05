CREATE TABLE CNTR_STDZN_TERM_DFNTN_TB(
                                 CNTR_ID char(2),
                                 CNTR_NM varchar(30),
                                 PREV_TB_NM varchar(50),
                                 PREV_CLMN_NM varchar(50) ,
                                 KOR_TERM varchar(500),
                                 ENG_TERM varchar(500),
                                 ENG_ABB_NM varchar(50),
								 TERM_EXPLN varchar(500),
								 DMN_LRG_CTGRY varchar(20),
								 DMN_MDDL_CTGRY varchar(20),
								 KOR_DMN_NM varchar(100),
                                 CREATE_TIME varchar(14),
                                 UPDATE_TIME varchar(14),
								 PRIMARY KEY(CNTR_ID, PREV_TB_NM, ENG_ABB_NM, CREATE_TIME )
                            )

