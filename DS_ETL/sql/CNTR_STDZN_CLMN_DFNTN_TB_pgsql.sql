CREATE TABLE CNTR_STDZN_CLMN_DFNTN_TB(
                                 CNTR_NM varchar(30),
                                 CNTR_ID char(2) not null,
                                 PRD_CD varchar(30),
                                 PRD_NM varchar(200),
                                 PREV_TB_NM varchar(50),
                                 DTST_CD varchar(50),
                                 NW_TB_NM varchar(50) not null,
                                 IDX varchar(3),
                                 PREV_CLMN_NM varchar(50),
                                 KOR_CLMN_NM varchar(100),
                                 NW_CLMN_NM varchar(50) not null,
                                 DMN_LRG_CTGRY varchar(20),
                                 DMN_MDDL_CTGRY varchar(20),
                                 KOR_DMN_NM varchar(100),
								 TYPE_LNTH varchar(20),
								 DATA_FRMT varchar(50),
                                 NOTNULL_YN varchar(8),
                                 MINM_VL varchar(20),
								 MAX_VL varchar(20),
								 XCPTN_VL varchar(500),
                                 CLMN_FUNC varchar(500),
                                 DATA_CONT_FUNC varchar(500),
                                 RMKS varchar(500),
                                 CREATE_TIME varchar(14),
                                 UPDATE_TIME varchar(14),
								 PRIMARY KEY (CNTR_ID, PRD_CD, NW_TB_NM, IDX, NW_CLMN_NM, CREATE_TIME)
                             )
