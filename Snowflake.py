
class SCDHandlerSnowflake():
    """
    Help Incremental load into a table using different SCD approach.
    currently: SCD Type 0, Type 1 and Type 2 has been implemented.
    Feel free to add to this class.
    """

    def __init__(self,
                 data: pd.DataFrame,
                 table_name: str,
                 db_config: dict,
                 stage_name: str,
                 primary_keys: [str, List[str]] = 'id',
                 file_format_name=None,
                 column_mapping: Optional[Dict[str, str]] = None,
                 encoding='utf-8',
                 field_delimiter='|',
                 skip_header=1,
                 ftype='CSV',
                 field_optionally_enclosed_by='"',
                 extra_format_options={},
                 ):
        """

        :param data as form of Dataframe:
        :param table_name:
        :param query - if any custom Query needs to be run:
        :param column_mapping - if the column name on the 'data' df is not same as the 'table_name':
        """
        self.data = data
        self.stage_name = stage_name
        self.encoding = encoding
        self.columns = list(self.data.columns)
        self.table_name = table_name
        self.primary_keys = [primary_keys] if type(primary_keys) is str else primary_keys
        self.file_format_name = file_format_name
        self.file_format = file_format_name
        self.db_config = db_config
        self.column_mapping = column_mapping
        self.logger = logger
        self.format_options = {**extra_format_options, **dict(skip_header=skip_header,
                                                              field_optionally_enclosed_by=field_optionally_enclosed_by,
                                                              field_delimiter=field_delimiter,
                                                              type=ftype,
                                                              encoding=encoding)}
        self.format_options_q = '\n'.join(
            [f"{k.upper()} = '{v}'" if isinstance(v, str) else f"{k.upper()} = {v}"
             for k, v in self.format_options.items()]
        )
        self.file_format = file_format_name or self._create_file_format()
        self.surrogate_cols = ['_is_current', '_end_date', '_created_date']

    @staticmethod
    def _get_sf_connection(func):
        "Decorator to inject conn and close it after usage"

        def wrapper(self, *args, **kwargs):
            with new_connection(**self.db_config) as conn:
                try:
                    # if 'conn' not in [kwargs.keys()] + args:
                    return func(self, *args, **kwargs, conn=conn)
                    # else:
                    # return func(self, *args, **kwargs)
                except Exception as e:
                    logger.error(str(e))
                    conn.rollback()
                    raise
                finally:
                    conn.close()

        return wrapper

    @_get_sf_connection
    def _create_file_format(self, conn):
        if not self.file_format:
            self.file_format = 'my_temp_csv_format'
            q = f"""
            CREATE OR REPLACE FILE FORMAT {self.file_format}
            {self.format_options_q};
            """
            logger.debug(f'Query: \n{q}')
            run_query(q, conn=conn)
            logger.debug(f'TEMP File format {self.file_format} created')
        return self.file_format

    def _generate_gzip_buffer(self, data):
        """convert the data to a gzip buffer from a dataframe to stage in Snowflake
           Stage DataFrame to Snowflake using in-memory file object
        """

        csv_buffer = io.StringIO()
        gzip_buffer = io.BytesIO()

        data.to_csv(csv_buffer, sep='|', index=False)
        csv_data = csv_buffer.getvalue()
        with gzip.GzipFile(fileobj=gzip_buffer, mode='wb') as gz:
            gz.write(csv_data.encode(self.encoding))

        gzip_buffer.seek(0)
        return gzip_buffer

    def _write_buffer_to_file(self, buffer, format='gz'):
        temp_dir = tempfile.gettempdir()
        temp_file_name = f'temp_stage_{self.table_name}_{dt.datetime.now().strftime("%Y%m%d_%H%M%S")}.{format}'
        temp_file_path = os.path.join(temp_dir, temp_file_name)
        with open(temp_file_path, 'wb') as f:
            f.write(buffer.read())
        return temp_file_path, temp_file_name

    @_get_sf_connection
    def _stage_df_to_snowflake(self, data: pd.DataFrame, conn: SnowflakeConnection):
        buffer = self._generate_gzip_buffer(data)
        filepath, filename = self._write_buffer_to_file(buffer)
        stage_file(filepath, stage_name=self.stage_name, conn=conn)
        return filename

    @_get_sf_connection
    def _cleanup(self, filename, conn: SnowflakeConnection):
        del_q = f"REMOVE @{self.stage_name} PATTERN='.*{filename}.*'"
        conn.cursor().execute(del_q)
        logger.info(del_q)
        if self.file_format_name is not None:
            del_q2 = f"DROP FILE FORMAT IF EXISTS {self.file_format}"
            conn.cursor().execute(del_q2)
            logger.info(del_q2)

    @_get_sf_connection
    def _generate_scd_type0_sql(self, staged_file, conn=None) -> str:
        """
        Generate dynamic SQL for SCD Type 0 load
        """
        columns_str = ', '.join(self.columns)
        placeholders = ', '.join([f'src.{col}' for col in self.columns])
        where_conditions = []
        for i, col in enumerate(self.primary_keys):
            where_conditions.append(f"trgt.{col} = src.{col}")

        where_clause = ' AND '.join(where_conditions)
        sql = f"""
        MERGE INTO {self.table_name} AS trgt
        USING (
            SELECT 
                {', '.join([f'${i + 1} AS {col}' for i, col in enumerate(self.columns)])}
            FROM @{self.stage_name}/{staged_file}
            (FILE_FORMAT => {self.file_format} )
        ) AS src
        ON {where_clause}
        WHEN NOT MATCHED THEN 
            INSERT ({columns_str})
            VALUES ({placeholders})
        ;
        """
        logger.debug(f'Query: \n{sql}')
        return sql

    @_get_sf_connection
    def scd_type0(self, conn, table_name=None):
        if table_name is not None:
            self.table_name = table_name
        staged_file = self._stage_df_to_snowflake(self.data)
        query = self._generate_scd_type0_sql(staged_file)
        result = run_query(query, conn=conn, return_results=True)
        self._cleanup(staged_file)

        logger.info(f'SCD type0 data load completed. table: {self.table_name}')
        logger.info(f'{self.table_name} -> {":".join(f'{k}:{v}' for k, v in result[0].items())} ')
        return staged_file

    @_get_sf_connection
    def scd_type1(self, conn, table_name=None):
        if table_name is not None:
            self.table_name = table_name
        staged_file = self._stage_df_to_snowflake(self.data)
        query = self._generate_scd_type1_sql(staged_file)
        result = run_query(query, conn=conn, return_results=True)
        self._cleanup(staged_file)

        logger.info(f'SCD type1 data load completed. table: {self.table_name}')
        logger.info(f'{self.table_name} -> {":".join(f'{k}:{v}' for k, v in result[0].items())} ')
        return staged_file

    @_get_sf_connection
    def _generate_scd_type1_sql(self, staged_file, conn=None) -> str:
        """
        Generate dynamic SQL for SCD Type 1 load
        """
        columns_str = ', '.join(self.columns)
        placeholders = ', '.join([f'src.{col}' for col in self.columns])
        where_conditions = []
        update_cols = []
        for i, col in enumerate(self.primary_keys):
            where_conditions.append(f"trgt.{col} = src.{col}")

        for i, col in enumerate(self.columns):
            update_cols.append(f"trgt.{col} = src.{col}")

        where_clause = ' AND '.join(where_conditions)
        update_clause = ','.join(update_cols)
        sql = f"""
            MERGE INTO {self.table_name} AS trgt
            USING (
            SELECT 
            {', '.join([f'${i + 1} AS {col}' for i, col in enumerate(self.columns)])}
            FROM @{self.stage_name}/{staged_file}
            (FILE_FORMAT => {self.file_format} )
            ) AS src
            ON {where_clause}
            WHEN MATCHED THEN
            UPDATE SET
            {update_clause}
            WHEN NOT MATCHED THEN 
            INSERT ({columns_str})
            VALUES ({placeholders})
            ;
            """
        logger.debug(f'Query: \n{sql}')
        return sql

    @_get_sf_connection
    def _get_col_names(self, conn):
        q = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE
            table_name = '{self.table_name}'
            AND table_schema = '{self.db_config["schema"]}'
            """
        logger.debug(f'Query: \n{q}')
        result = run_query(q, conn=conn, return_results=True)
        return [res['COLUMN_NAME'] for res in result]

    @_get_sf_connection
    def _alter_table(self, table_col_names, conn):
        for col in self.surrogate_cols:
            if col.upper() not in [x.upper() for x in table_col_names]:
                if col.endswith('date'):
                    q = f"""
                        ALTER TABLE {self.table_name}
                        ADD COLUMN IF NOT EXISTS {col} TIMESTAMP_NTZ;
                        """
                    logger.debug(f'Query: \n{q}')
                elif col.endswith('_current'):
                    q = f"""
                        ALTER TABLE {self.table_name}
                        ADD COLUMN IF NOT EXISTS {col} CHAR(3);
                        """
                logger.debug(f'Query: \n{q}')
                result = run_query(q, conn=conn, return_results=True)
                logger.debug(f'\n{result}')
        logger.info(f'Table {self.table_name} Altered to include surrogate columns')

    @_get_sf_connection
    def _get_column_types(self, conn):
        """Get column names and their data types from the table"""
        q = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = '{self.table_name}'
            AND table_schema = '{self.db_config["schema"]}'
            ORDER BY ORDINAL_POSITION
        """
        logger.debug(f'Query: \n{q}')
        result = run_query(q, conn=conn, return_results=True)
        return {res['COLUMN_NAME']: res['DATA_TYPE'] for res in result}

    def _get_cast_expression(self, column_name, data_type, param_index, ref='$'):
        """Generate appropriate TRY_CAST for use in staged files."""
        sf_data_type = data_type.upper()
        known_types = [
            'NUMBER', 'INTEGER', 'FLOAT', 'DOUBLE', 'VARCHAR', 'STRING', 'TEXT', 'CHAR',
            'BOOLEAN', 'DATE', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ',
            'TIME', 'BINARY', 'VARIANT', 'OBJECT', 'ARRAY'
        ]
        if sf_data_type not in known_types:
            sf_data_type = 'VARCHAR'
        return f'TRY_CAST({ref}{param_index if ref == '$' else column_name} AS {sf_data_type})'

    @_get_sf_connection
    def _generate_dynamic_scd_type2_sql(self, staged_file, conn) -> str:
        column_types = self._get_column_types()
        columns = list(column_types.keys())
        column_list_str = ', '.join(columns)
        # SELECT clause
        select_clauses = []
        for i, col in enumerate(columns):
            data_type = column_types.get(col, 'VARCHAR')
            select_clauses.append(f"{self._get_cast_expression(column_name=col, data_type=data_type, param_index=i + 1, ref='$')} AS {col}")
        select_clause = ',\n        '.join(select_clauses)

        # Join conditions
        join_conditions = []
        existing_conditions = []
        for i, pk in enumerate(self.primary_keys):
            data_type = column_types.get(pk, 'VARCHAR')
            join_expr = self._get_cast_expression(column_name=pk, data_type=data_type, param_index=i + 1, ref='src.')
            join_conditions.append(f"trgt.{pk} = {join_expr}")
            existing_conditions.append(f"existing.{pk} = {join_expr}")
        join_clause = ' AND '.join(join_conditions)
        existing_clause = ' AND '.join(existing_conditions)

        # HASH expressions
        hash_expr = f"HASH({', '.join([self._get_cast_expression(column_name=col, data_type=column_types.get(col, 'VARCHAR'), param_index=idx + 1, ref='src.') for idx, col in enumerate(self.columns)])})"
        hash_existing_expr = f"HASH({', '.join([f'trgt.{col}' for col in self.columns])})"

        # VALUES clause
        insert_values_clause = ', '.join([f"src.{col}" for col in columns])

        today_str = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')

        q1 = f"""
        MERGE INTO TEST_SCD AS trgt
        USING (
            SELECT
               {select_clause}
            FROM @CII_STAGE/temp_stage_TEST_SCD_20250801_091956.gz
            (FILE_FORMAT => my_temp_csv_format)
        ) AS src
        ON {join_clause} AND trgt._is_current = 'Y'
        WHEN MATCHED AND {hash_expr} != {hash_existing_expr} THEN
        UPDATE SET
            trgt._is_current = 'N',
            trgt._end_date = '{today_str}';

        """
        q2 = f"""
        MERGE INTO TEST_SCD AS trgt
        USING (
            SELECT
                {select_clause}
            FROM @CII_STAGE/temp_stage_TEST_SCD_20250801_091956.gz
            (FILE_FORMAT => my_temp_csv_format)
        ) AS src
        ON {join_clause} AND trgt._is_current = 'Y'
        WHEN NOT MATCHED THEN
        INSERT ({column_list_str})
        VALUES (
            {insert_values_clause}
        );
        """
        logger.info(f'Query1 \n{q1}')
        logger.info(f'Query2 \n{q2}')
        return q1, q2

    @_get_sf_connection
    def scd_type2(self, conn, table_name=None):
        if table_name is not None:
            self.table_name = table_name
        data = self.data.copy()
        data['_is_current'] = 'Y'
        data['_end_date'] = dt.date(year=2100, month=1, day=1)
        data['_created_date'] = dt.datetime.today()
        staged_file = self._stage_df_to_snowflake(data)
        table_col_names = self._get_col_names()

        self._alter_table(table_col_names=table_col_names)
        q1, q2 = self._generate_dynamic_scd_type2_sql(staged_file=staged_file)
        result1 = run_query(q1, conn=conn, return_results=True)
        logger.info(result1)
        result2 = run_query(q2, conn=conn, return_results=True)
        logger.info(result2)
        return result1, result2
        self._cleanup(staged_file)

        logger.info(f'SCD type2 data load completed. table: {self.table_name}')
        logger.info(f'{self.table_name} -> {":".join(f'{k}:{v}' for k, v in result1[0].items())} ')
        logger.info(f'{self.table_name} -> {":".join(f'{k}:{v}' for k, v in result2[0].items())} ')
        return staged_file


if __name__ == '__main__':
    data = pd.DataFrame(data=[[1, 2, 3], [3, 4, 6], [7, 9, 9], [10, 11, 12], [13, 14, 15], [16, 17, 18]], columns=['A', 'B', 'C'])
    upsert = SCDHandlerSnowflake(
        data=data,
        table_name='TEST_SCD',
        db_config={
            "user": os.getenv("SF_USER"),
            "password": os.getenv("SF_PASSWD"),
            "account": os.getenv("SF_ACCT"),
            "warehouse": os.getenv("SF_WAREHOUSE"),
            "database": os.getenv("SF_DB"),
            "schema": os.getenv("SF_SCHEMA")
        },
        stage_name='CII_STAGE',
        primary_keys='A',
    )
    # upsert.scd_type0()
    print(upsert._get_column_types())
    print(upsert.scd_type2())
