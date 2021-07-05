CREATE TABLE CNTR_STDZN_DMN_DFNTN_TB(
                                 DMN_LRG_CTGRY varchar(20),
                                 DMN_MDDL_CTGRY varchar(20),
                                 KOR_DMN_NM varchar(100),
                                 ENG_DMN_MDDL_CTGRY_NM varchar(200) ,
                                 ENG_DMN_NM varchar(200),
                                 DATA_TYPE varchar(10),
                                 DATA_LNTH varchar(10),
                                 MSRM_UNIT varchar(100),
                                 DMN_FRMT varchar(50),
								 DMN_EXPLN varchar(500),
								 CREATE_TIME varchar(14),
								 UPDATE_TIME varchar(14),
								 PRIMARY KEY(DMN_LRG_CTGRY, DMN_MDDL_CTGRY, KOR_DMN_NM, CREATE_TIME)
                            )

