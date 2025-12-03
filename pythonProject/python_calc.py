# python_calc.py

import re
import time
import urllib.parse

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

SERVER_NAME = r"DESKTOP-D01DQH2\SQLEXPRESS"
DATABASE_NAME = 'PaymentFormulasDB'
DRIVER = 'ODBC Driver 17 for SQL Server'
METHOD_NAME = 'Python_Pandas_Eval_Fixed_Chunks_Safe_v2'

def build_sqlalchemy_url(server: str, database: str, driver: str) -> str:
    params = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )
    quoted = urllib.parse.quote_plus(params)
    return f"mssql+pyodbc:///?odbc_connect={quoted}"

def enable_fast_executemany(engine):
    try:
        rc = engine.raw_connection()
        cursor = rc.cursor()
        if hasattr(cursor, 'fast_executemany'):
            cursor.fast_executemany = True
        cursor.close()
        rc.close()
    except Exception:
        pass

def transform_expression(sql_expr: str) -> str:
    if sql_expr is None:
        return None
    expr = str(sql_expr)
    replacements = [
        (r"\bSQRT\s*\(", "np.sqrt("),
        (r"\bPOWER\s*\(", "np.power("),
        (r"\bLN\s*\(", "np.log("),
        (r"\bABS\s*\(", "np.abs("),
        (r"\bEXP\s*\((.*?)\)", r"np.exp(np.clip(\1, -700, 700))"),
        (r"\bROUND\s*\(", "np.round("),
    ]
    for patt, repl in replacements:
        expr = re.sub(patt, repl, expr, flags=re.IGNORECASE)
    return expr.strip()

def transform_condition(tnai: str) -> str:
    if tnai is None:
        return None
    cond = str(tnai)
    cond = re.sub(r"\bAND\b", "&", cond, flags=re.IGNORECASE)
    cond = re.sub(r"\bOR\b", "|", cond, flags=re.IGNORECASE)
    cond = re.sub(r"(?<![<>=!])=(?!=)", "==", cond)
    cond = cond.replace("<>", "!=")
    return cond.strip()

def get_dataframes(engine):
    print("שולף נתונים ונוסחאות ממסד הנתונים...")
    df_data = pd.read_sql('SELECT data_id, a, b, c, d FROM data_t', engine)
    df_targil = pd.read_sql("SELECT targil_id, targil, tnai, false_targil FROM targil_t", engine)
    return df_data, df_targil

def calculate_formulas(df_data: pd.DataFrame, df_targil: pd.DataFrame, engine):
    results_list = []
    log_list = []

    # ניקוי רשומות קודמות
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DELETE FROM results_t WHERE method = :m"), {"m": METHOD_NAME})
            conn.execute(text(f"DELETE FROM log_t WHERE method = :m"), {"m": METHOD_NAME})
            conn.commit()
        print(" תוצאות קודמות נוקו.")
    except Exception as e:
        print(f" שגיאה בניקוי רשומות קודמות: {e}")

    df_data[['a','b','c','d']] = df_data[['a','b','c','d']].apply(pd.to_numeric, errors='coerce').fillna(0)

    for _, row in df_targil.iterrows():
        targil_id = int(row['targil_id'])
        targil_raw = row['targil']
        tnai_raw = row['tnai']
        false_raw = row['false_targil']

        print(f"\n מעבד נוסחה ID: {targil_id}, נוסחה: {targil_raw}")

        expr = transform_expression(targil_raw)
        false_expr = transform_expression(false_raw) if pd.notnull(false_raw) else None
        tnai_expr = transform_condition(tnai_raw) if pd.notnull(tnai_raw) else None

        start_time = time.time()
        local_dict = {col: df_data[col].values for col in ['a','b','c','d']}
        local_dict['np'] = np

        try:
            with np.errstate(divide='ignore', invalid='ignore', over='ignore'):
                if tnai_expr:
                    mask = np.array(eval(tnai_expr, {"np": np}, local_dict), dtype=bool)
                    true_vals = eval(expr, {"np": np}, local_dict)
                    false_vals = eval(false_expr, {"np": np}, local_dict) if false_expr else np.zeros(len(df_data))
                    calculated_results = np.where(mask, true_vals, false_vals)
                else:
                    calculated_results = eval(expr, {"np": np}, local_dict)

            # המרה של ערכים לא מספריים ל-0 או np.nan
            calculated_results = np.nan_to_num(calculated_results, nan=0.0, posinf=1e10, neginf=-1e10)

        except Exception as e:
            print(f" שגיאה בחישוב נוסחה {targil_id}: {e}")
            continue

        duration = time.time() - start_time

        temp_results_df = pd.DataFrame({
            'data_id': df_data['data_id'].values,
            'targil_id': targil_id,
            'method': METHOD_NAME,
            'result': calculated_results
        })
        results_list.append(temp_results_df)
        log_list.append({'targil_id': targil_id, 'method': METHOD_NAME, 'time_run': float(duration)})
        print(f" נוסחה {targil_id} חישבה {len(calculated_results)} רשומות בזמן {duration:.3f} שניות")

    # Bulk insert ב־chunks בטוחים
    if results_list:
        final_results_df = pd.concat(results_list, ignore_index=True)
        print(f"\n סיום חישוב. מכניס {len(final_results_df)} רשומות ל-results_t...")

        try:
            chunk_size = 500  # מספר רשומות קטן יותר כדי למנוע COUNT field error
            with engine.begin() as conn:  # context manager עם commit אוטומטי
                for start in range(0, len(final_results_df), chunk_size):
                    end = start + chunk_size
                    chunk = final_results_df.iloc[start:end]
                    chunk.to_sql('results_t', conn, if_exists='append', index=False)
            print(" תוצאות נשמרו בהצלחה ב-results_t.")
        except Exception as e:
            print(f" שגיאה בהכנסת נתונים: {e}")

    if log_list:
        df_log = pd.DataFrame(log_list)
        try:
            df_log.to_sql('log_t', engine, if_exists='append', index=False)
            print(" זמני ריצה נשמרו ב-log_t.")
        except Exception as e:
            print(f" שגיאה בהכנסת נתוני לוג: {e}")

def main():
    url = build_sqlalchemy_url(SERVER_NAME, DATABASE_NAME, DRIVER)
    engine = create_engine(url, pool_pre_ping=True)
    enable_fast_executemany(engine)

    try:
        df_data, df_targil = get_dataframes(engine)
        calculate_formulas(df_data, df_targil, engine)
    except Exception as e:
        print(f" קרתה שגיאה בתהליך הראשי: {e}")
    finally:
        try:
            engine.dispose()
        except Exception:
            pass

if __name__ == '__main__':
    main()
