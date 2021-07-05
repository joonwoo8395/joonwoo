CREATE TABLE CNTR_STDZN_TERM_DFNTN_TB(
                                 CNTR_ID TEXT,
                                 CNTR_NM TEXT,
                                 PREV_TB_NM TEXT,
                                 PREV_CLMN_NM TEXT ,
                                 KOR_TERM TEXT,
                                 ENG_TERM TEXT,
                                 ENG_ABB_NM TEXT,
								 TERM_EXPLN TEXT,
								 KOR_DMN_LRG_CTGRY TEXT,
								 KOR_DMN_MDDL_CTGRY TEXT,
								 KOR_DMN_SMLL_CTGRY TEXT,
                                 CREATE_TIME TEXT,
                                 UPDATE_TIME TEXT
                            )
							datascope GLOBAL
							ramexpire 0
							diskexpire 0
							partitionkey None
							partitiondate None
							partitionrange 0;

