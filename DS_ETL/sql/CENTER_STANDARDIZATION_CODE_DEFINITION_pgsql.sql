CREATE TABLE public.CENTER_STANDARDIZATION_CODE_DEFINITION(
                                 CNTER_ID char(2) NOT NULL,
                                 CNTER_NM varchar(30),
                                 CODE_NAME varchar(40),
                                 CODE_VALUE varchar(20) NOT NULL,
                                 KOR_CODE_VALUE_MEANING varchar(200),
                                 ENG_CODE_VALUE_MEANING varchar(200),
                                 EXPLANATION varchar(100),
                                 CREATE_TIME varchar(14),
                                 UPDATE_TIME varchar(14),
                                 PRIMARY KEY(CNTER_ID,CODE_NAME, CODE_VALUE,CREATE_TIME)
							)

