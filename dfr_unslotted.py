from os import path
from datetime import datetime
import psycopg2
import psycopg2.extras
import xlsxwriter as xl

class Solve_DFR_Service_History():
    
    def __init__(self):
        """Connects to APPS Database
        """
        self.db_params = {
            'database': 'apps_moi_testing_15_march',
            'user': 'itadmin',
            'password': 'Passw0rd@1234567',
            'host': '172.30.200.100',
            'port': 5432
        }
        try:
            self.db_connection = psycopg2.connect(**self.db_params)
            self.cursor = self.db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            print('\033[1;32m ===== Connected to Database ====\033[1;m')
        except Exception as e:
            print('Connection to database failed! \n', str(e))
            exit()
            
    def __from_DB(self, table_name):
        """Selects DFR records based on table name
        """
        try:
            self.cursor.execute("""SELECT * FROM %s """ % table_name)
            return self.cursor.fetchall()
            
        except Exception as e:
            print('\nDatabase Error or Invalid table name: \n', str(e))
            exit()
    
    def __create_excel_report(self, sheet_name, total_updated):
        """Creates an excel report of problematic records
        """
        date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        workbook = xl.Workbook(sheet_name +' '+ date +'.xlsx')
        sheet = workbook.add_worksheet(sheet_name)
        sheet.set_column('A:A', 22)
        sheet.set_column('B:B', 70)
        sheet.set_column('C:C', 22)
        sheet.set_column('D:D', 22)
        sheet.set_column('E:E', 22)
        sheet.set_column('F:F', 22)
        wb_format = workbook.add_format()
        wb_format.set_bold()
        sheet.write(0, 0, 'APPS ID', wb_format)
        sheet.write(0, 1, 'Remarks', wb_format)
        sheet.write(0, 2, 'Name', wb_format)
        sheet.write(0, 3, 'Father Name', wb_format)
        sheet.write(0, 4, 'نام', wb_format)
        sheet.write(0, 5, 'نام پدر', wb_format)
        row = 1
        col = 0
        for rec in self.result:
            if rec.get('emp_id', False):
                self.cursor.execute("""SELECT name,d_name,father_name, d_father_name FROM hr_employee WHERE id = %s""", (rec.get('emp_id'),))
                emp = self.cursor.fetchone()
                sheet.write(row, col, rec.get('apps_id'))
                sheet.write(row, col+1, rec.get('msg'))
                sheet.write(row, col+2, emp.get('name'))
                sheet.write(row, col+3, emp.get('father_name'))
                sheet.write(row, col+4, emp.get('d_name'))
                sheet.write(row, col+5, emp.get('d_father_name'))
            else:
                sheet.write(row, col, rec.get('apps_id'))
                sheet.write(row, col+1, rec.get('msg'))
            row += 1
        workbook.close()
        self.result = []
        
    
    def get_enrollment_date(self, apps_id):
        self.cursor.execute("""SELECT date_joined_the_force AS enrol_date FROM hr_employee WHERE apps_id = %s AND active='t' LIMIT 1""",
                                (apps_id,))
        return self.cursor.fetchone()
    
    def get_dfr_date(self, emp_id):
        self.cursor.execute("""SELECT 
                                dfr.effective_date, dfr.id, awol.with_weapon
                            FROM 
                                dfr_status dfr
                            INNER JOIN 
                                awol_duty awol ON awol.id = dfr.awol_duty_id
                            WHERE 
                                dfr.hr_employee_id = %s 
                            AND 
                                dfr.active = 't' 
                            AND 
                                dfr.state in ('approved_by_hr', 'approved_by_grc', 'approved_by_prc_gdop')
                            AND 
                                dfr.effective_date IS NOT NULL
                            AND awol.active = 't'""", (emp_id,))
        res= self.cursor.fetchall()
        return res

                
    def __process_not_slotted_with_dfr_without_service(self, table_name, sheet_name): #4
        """Process not slotted records with dfr and without service
        """
        records = self.__from_DB(table_name)
        counter = 0
        for rec in records:
            err = {}
            apps_id = rec.get('apps_id')
            if not apps_id:
                continue
            emp_id = self.__get_hr_employee_id(apps_id)
            enrol_date = self.get_enrollment_date(apps_id)
            if not emp_id:
                print('Employee is slotted')
                self.result.append({'apps_id': str(apps_id), 'msg': 'Employee is slotted / پرسونل تعیین بست است'})
                continue
            dfr_date = self.get_dfr_date(emp_id)
            if not dfr_date:
                self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'there is no approved awol dfr record for employee / هیچ ریکارد فعال غیرحاضر یافت نشد dfr'})
                print('There is no approved AWOL DFR record')
                continue
            if len(dfr_date) > 1:
                self.result.append({'apps_id': str(apps_id), 'msg': 'Employee has more than 1 approved dfr / بیش از یک ریکارد فعال یافت شد dfr'})
                print('Employee has more than 1 approved DFR record')
                continue
            dfr_id = dfr_date[0].get('id')
            with_weapon = dfr_date[0].get('with_weapon')
            dfr_date = dfr_date[0].get('effective_date')
            if enrol_date and enrol_date.get('enrol_date'):
                enrol_date = enrol_date.get('enrol_date')
                try:
                    self.cursor.execute("BEGIN")
                    self.cursor.execute("""SELECT end_date, id FROM employee_service_history WHERE hr_employee_id=%s AND active='t'""", (emp_id,))
                    res = self.cursor.fetchone()
                    if res and res.get('id'):
                        print('Service history already exist')
                        self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Service history already exist / تاریخ خدمت ازقبل موجود است'})
                        self.db_connection.rollback()
                        continue                    
                    self.cursor.execute("""SELECT to_date, id FROM assignment_assignment WHERE hr_employee_id = %s 
                                        AND active='t' AND to_date IS NOT NULl AND from_date < %s AND state = 'unslotted' ORDER BY to_date DESC LIMIT 1""", (emp_id, str(dfr_date)))
                    last_assign_end_date = self.cursor.fetchone()
                    if not last_assign_end_date or not last_assign_end_date.get('to_date'):
                        self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'There is no unslotted assignment before DFR effective date / هیچ تعیین بست نیست قبل از تاریخ dfr'})
                        self.db_connection.rollback()
                        print('There is no unslotted assignment before DFR effective date')
                        continue
                    if last_assign_end_date.get('to_date') == dfr_date:
                        dfr_reson = 'AWOL - Without Weapon'
                        d_dfr_reson = 'غیرحاضری خودسرانه - بدون اسلحه'
                        if with_weapon:
                            dfr_reson = 'AWOL - With Weapon'
                            d_dfr_reson = 'غیرحاضری خودسرانه -همراه با اسلحه'
                        self.cursor.execute("""INSERT INTO employee_service_history (hr_employee_id, start_date, end_date, dfr_id, dfr_date, separation_reson, d_separation_reson, active, create_date)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, 't', %s)""", (emp_id,  str(enrol_date), str(dfr_date), dfr_id, str(dfr_date), dfr_reson, d_dfr_reson, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
                        
                    else:
                        self.cursor.execute("""UPDATE assignment_assignment SET to_date = %s WHERE id = %s""", (str(dfr_date), last_assign_end_date.get('id')))
                        dfr_reson = 'AWOL - Without Weapon'
                        d_dfr_reson = 'غیرحاضری خودسرانه - بدون اسلحه'
                        if with_weapon:
                            dfr_reson = 'AWOL - With Weapon'
                            d_dfr_reson = 'غیرحاضری خودسرانه -همراه با اسلحه'
                        self.cursor.execute("""INSERT INTO employee_service_history (hr_employee_id, start_date, end_date, dfr_id, dfr_date, separation_reson, d_separation_reson, active, create_date)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, 't', %s)""", (emp_id,  str(enrol_date), str(dfr_date), dfr_id, str(dfr_date), dfr_reson, d_dfr_reson, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
                        
                        if dfr_date > last_assign_end_date.get('to_date'): # T & A
                            start = last_assign_end_date.get('to_date')
                            end = dfr_date
                        else:
                            start = dfr_date
                            end = last_assign_end_date.get('to_date')
                        self.cursor.execute("""DELETE FROM personnel_time_attendance WHERE hr_employee_id = %s
                                    AND date >= %s AND date <= %s""", (emp_id, start, end))
                    
                    
                    self.cursor.execute("""SELECT from_date FROM assignment_assignment WHERE hr_employee_id = %s  
                                        AND from_date > %s AND active='t' AND state = 'unslotted' ORDER BY from_date LIMIT 1""",(emp_id, str(dfr_date)))
                    assignment_after_dfr = self.cursor.fetchone()
                    if not assignment_after_dfr or not assignment_after_dfr.get('from_date'): # AWOL
                        d_active_duty_status = 'غیر فعال'
                        d_active_duty_status += ' | منفک (غیر حاضری خود سرانه)'
                        self.cursor.execute("""UPDATE hr_employee SET active_duty_status = %s, d_active_duty_status = %s 
                                            WHERE id = %s returning id, ahrims_sync_id""", 
                                            ('Inactive | DFR (AWOL)', d_active_duty_status, emp_id))
                        self.__update_ahrims_sync_id('hr_employee', self.cursor.fetchone())
                    else:
                        self.cursor.execute("""INSERT INTO employee_service_history (hr_employee_id, start_date, active, create_date)
                                        VALUES (%s, %s, 't', %s)""", (emp_id,  str(assignment_after_dfr.get('from_date')), datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
                        self.cursor.execute("""UPDATE assignment_assignment SET state = 'cancelled' WHERE hr_employee_id = %s
                                            AND from_date > %s AND active = 't' AND state NOT IN ('slotted', 'unslotted')""", (emp_id, str(dfr_date)))
                        self.cursor.execute("""SELECT id FROM duty_status WHERE name = 'Present' AND active='t' LIMIT 1""")
                        present_rec = self.cursor.fetchone()
                        self.cursor.execute("""UPDATE personnel_time_attendance SET duty_status_id = %s, is_duty_status_readonly = 'f' 
                                            WHERE date >= %s AND hr_employee_id = %s""", (present_rec.get('id'), str(assignment_after_dfr.get('from_date')), emp_id))
                    self.db_connection.commit()
                    # self.db_connection.rollback()
                    counter += 1
                    self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Issue Fixed / مشکل حل گردید'})
                    print(table_name+': '+str(counter) + ' Done!')
                except Exception as e:
                    self.db_connection.rollback()
                    print(e)
                    if 'overlap' in str(e):
                        err = {'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Service history already exist / تاریخ خدمت ازقبل موجود است'}
                    else:
                        err = {'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': str(e)}
                    self.result.append(err)
                    
            else:
                self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Enrollment date is empty / تاریخ نشات ندارد'})
                print('Enrollment date not found')
        print('\n\n----------- Total updated records '+str(counter))
        self.__create_excel_report(sheet_name, counter)
        
        
    def __process_not_slotted_without_service_dfr(self, table_name, sheet_name): #5
        """Process not slotted records without service and dfr
        """
        records = self.__from_DB(table_name)
        counter = 0
        for rec in records:
            err = {}
            apps_id = rec.get('apps_id')
            if not apps_id:
                continue
            enrol_date = self.get_enrollment_date(apps_id)
            emp_id = self.__get_hr_employee_id(apps_id)
            if not emp_id:
                print('Employee is slotted')
                self.result.append({'apps_id': str(apps_id), 'msg': 'Employee is slotted / پرسونل تعیین بست است'})
                continue
            self.cursor.execute("""SELECT id FROM employee_service_history WHERE hr_employee_id=%s AND active='t'""", (emp_id,))
            res = self.cursor.fetchone()
            if res and res.get('id'):
                print('Service history already exist')
                self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Service history already exist / تاریخ خدمت ازقبل موجود است'})
                continue
            self.cursor.execute("SELECT id FROM dfr_status WHERE hr_employee_id = %s AND active = 't'", (emp_id,))
            res = self.cursor.fetchone()
            if res and res.get('id'):
                print('DFR record already exist')
                self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'DFR record already exist / ریکارد ازقبل موجود است dfr'})
                continue
            if enrol_date and enrol_date.get('enrol_date'):
                enrol_date = enrol_date.get('enrol_date')
                try:
                    self.cursor.execute("BEGIN")
                    self.cursor.execute("""INSERT INTO employee_service_history (hr_employee_id, start_date, active, create_date)
                                    VALUES (%s, %s, 't', %s)""", (emp_id, str(enrol_date), datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
                    self.db_connection.commit()
                    # self.db_connection.rollback()
                    counter += 1
                    self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Issue Fixed / مشکل حل گردید'})
                    print(table_name+': '+str(counter) + ' Done!')
                except Exception as e:
                    self.db_connection.rollback()
                    print(e)
                    if 'overlap' in str(e):
                        err = {'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Service history already exist / تاریخ خدمت ازقبل موجود است'}
                    else:
                        err = {'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': str(e)}
                    self.result.append(err)
            else:
                self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Enrollment date is empty / تاریخ نشات ندارد'})
                print('Enrollment date not found')
        print('\n\n----------- Total updated records '+str(counter))
        self.__create_excel_report(sheet_name, counter)
        
        
            
            
    def __process_not_slotted_with_service_dfr(self, table_name, sheet_name): #6
        """Process not slotted records with service and dfr
        """
        records = self.__from_DB(table_name)
        counter = 0
        for rec in records:
            err = {}
            apps_id = rec.get('apps_id')
            if not apps_id:
                continue
            emp_id = self.__get_hr_employee_id(apps_id)
            if not emp_id:
                print('Employee is slotted')
                self.result.append({'apps_id': str(apps_id), 'msg': 'Employee is slotted / پرسونل تعیین بست است'})
                continue
            dfr_date = self.get_dfr_date(emp_id)
            if not dfr_date:
                self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'there is no approved awol dfr record for employee / هیچ ریکارد فعال غیرحاضر یافت نشد dfr'})
                print('There is no approved AWOL DFR record')
                continue
            if len(dfr_date) > 1:
                self.result.append({'apps_id': str(apps_id), 'msg': 'Employee has more than 1 approved dfr / بیش از یک ریکارد فعال یافت شد dfr'})
                print('Employee has more than 1 approved DFR record')
                continue
            dfr_id = dfr_date[0].get('id')
            with_weapon = dfr_date[0].get('with_weapon')
            dfr_date = dfr_date[0].get('effective_date')
            try:
                self.cursor.execute("BEGIN")
                self.cursor.execute("""SELECT id, end_date, start_date FROM employee_service_history WHERE hr_employee_id = %s
                                    AND active='t' ORDER BY id DESC LIMIT 1""", (emp_id,))
                service_end_date = self.cursor.fetchone()
                if not service_end_date or not service_end_date.get('id'):
                    self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Service history is empty / تاریخ خدمت ندارد'})
                    self.db_connection.rollback()
                    continue
                self.cursor.execute("""SELECT id, to_date FROM assignment_assignment WHERE hr_employee_id = %s
                                    AND to_date IS NOT NULL AND from_date < %s AND active='t' ORDER BY from_date DESC LIMIT 1""",( emp_id, str(dfr_date)))
                last_assign_end_date = self.cursor.fetchone()
                if not last_assign_end_date or not last_assign_end_date.get('id'):
                    self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'There is no assignment before dfr effective date or assignment end date is empty / تاریخ ختم تعیین بست خالی است یا هیچ ریکاردی قبل از نیست قبل از dfr'})
                    self.db_connection.rollback()
                    continue
                    
                if not service_end_date.get('end_date') or service_end_date.get('end_date') != dfr_date:
                    dfr_reson = 'AWOL - Without Weapon'
                    d_dfr_reson = 'غیرحاضری خودسرانه - بدون اسلحه'
                    if with_weapon:
                        dfr_reson = 'AWOL - With Weapon'
                        d_dfr_reson = 'غیرحاضری خودسرانه -همراه با اسلحه' 
                    self.cursor.execute("""UPDATE employee_service_history SET end_date = %s, dfr_id = %s, dfr_date = %s, separation_reson = %s, d_separation_reson = %s WHERE id = %s""", 
                                        (str(dfr_date), dfr_id, str(dfr_date), dfr_reson, d_dfr_reson, service_end_date.get('id')))
                if last_assign_end_date.get('to_date') == dfr_date:
                    self.cursor.execute("""SELECT from_date FROM assignment_assignment WHERE hr_employee_id = %s
                                AND from_date > %s AND active='t' AND state = 'unslotted' ORDER BY from_date LIMIT 1""", 
                                    (emp_id, str(dfr_date)))
                    assignment_after_dfr = self.cursor.fetchone()
                    if assignment_after_dfr and assignment_after_dfr.get('from_date'):
                        self.cursor.execute("""INSERT INTO employee_service_history (hr_employee_id, start_date, active, create_date)
                                        VALUES (%s, %s, 't', %s)""", (emp_id, str(assignment_after_dfr.get('from_date')), datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
                        self.cursor.execute("""UPDATE assignment_assignment SET state = 'cancelled' WHERE hr_employee_id = %s
                                        AND from_date > %s AND active = 't' AND state NOT IN ('slotted', 'unslotted')""", (emp_id, str(dfr_date)))
                        self.cursor.execute("""SELECT id FROM duty_status WHERE name = 'Present' AND active='t' LIMIT 1""")
                        present_rec = self.cursor.fetchone()
                        self.cursor.execute("""UPDATE personnel_time_attendance SET duty_status_id = %s, is_duty_status_readonly = 'f' 
                                            WHERE date >= %s AND hr_employee_id = %s""", (present_rec.get('id'), str(assignment_after_dfr.get('from_date')), emp_id))
                    
                    else:
                        # AWOL
                        d_active_duty_status = 'غیر فعال'
                        d_active_duty_status += ' | منفک (غیر حاضری خود سرانه)'
                        self.cursor.execute("""UPDATE hr_employee SET active_duty_status = %s, d_active_duty_status = %s 
                                            WHERE id = %s returning id, ahrims_sync_id""", 
                                            ('Inactive | DFR (AWOL)', d_active_duty_status, emp_id))
                else:
                    dfr_reson = 'AWOL - Without Weapon'
                    d_dfr_reson = 'غیرحاضری خودسرانه - بدون اسلحه'
                    if with_weapon:
                        dfr_reson = 'AWOL - With Weapon'
                        d_dfr_reson = 'غیرحاضری خودسرانه -همراه با اسلحه'
                        
                    self.cursor.execute("""UPDATE assignment_assignment SET to_date = %s WHERE id = %s""", (str(dfr_date), last_assign_end_date.get('id')))
                    if dfr_date > last_assign_end_date.get('to_date'): # T & A
                        start = last_assign_end_date.get('to_date')
                        end = dfr_date
                    else:
                        start = dfr_date
                        end = last_assign_end_date.get('to_date')
                    self.cursor.execute("""DELETE FROM personnel_time_attendance WHERE hr_employee_id = %s
                                AND date >= %s AND date <= %s""", (emp_id, start, end))
                    
                    self.cursor.execute("""SELECT from_date FROM assignment_assignment WHERE hr_employee_id = %s
                                AND from_date > %s AND active='t' AND state = 'unslotted' ORDER BY from_date LIMIT 1""", 
                                    (emp_id, str(dfr_date)))
                    assignment_after_dfr = self.cursor.fetchone()
                    if assignment_after_dfr and assignment_after_dfr.get('from_date'):
                        self.cursor.execute("""INSERT INTO employee_service_history (hr_employee_id, start_date, active, create_date)
                                        VALUES (%s, %s, 't', %s)""", (emp_id, str(assignment_after_dfr.get('from_date')), datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
                        self.cursor.execute("""UPDATE assignment_assignment SET state = 'cancelled' WHERE hr_employee_id = %s
                                        AND from_date > %s AND active = 't' AND state NOT IN ('slotted', 'unslotted')""", (emp_id, str(dfr_date)))
                        self.cursor.execute("""SELECT id FROM duty_status WHERE name = 'Present' AND active='t' LIMIT 1""")
                        present_rec = self.cursor.fetchone()
                        self.cursor.execute("""UPDATE personnel_time_attendance SET duty_status_id = %s, is_duty_status_readonly = 'f' 
                                            WHERE date >= %s AND hr_employee_id = %s""", (present_rec.get('id'), str(assignment_after_dfr.get('from_date')), emp_id))
                    
                    # AWOL
                    d_active_duty_status = 'غیر فعال'
                    d_active_duty_status += ' | منفک (غیر حاضری خود سرانه)'
                    self.cursor.execute("""UPDATE hr_employee SET active_duty_status = %s, d_active_duty_status = %s 
                                        WHERE id = %s returning id, ahrims_sync_id""", 
                                        ('Inactive | DFR (AWOL)', d_active_duty_status, emp_id))

                counter += 1
                self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Issue Fixed / مشکل حل گردید'})
                self.db_connection.commit()
                # self.db_connection.rollback()
                print(table_name+': '+str(counter) + ' Done!')
            except Exception as e:
                self.db_connection.rollback()
                print(e)
                if 'overlap' in str(e):
                    err = {'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Service history already exist / تاریخ خدمت ازقبل موجود است'}
                else:
                    err = {'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': str(e)}
                self.result.append(err)
        print('\n\n----------- Total updated records '+str(counter))
        self.__create_excel_report(sheet_name, counter)
                
            
    def __update_ahrims_sync_id(self, table_name, result):
        """Updates the ahrism_sync_id of a table to 4444444 if it is not set
        """
        if result and not result.get('ahrims_sync_id'):
            self.cursor.execute("""UPDATE %s SET ahrims_sync_id = 44444444 WHERE id = %%s""" % table_name, (result.get('id'),))
    
    
    def __get_hr_employee_id(self, apps_id):
        """returns hr_employee_id
        """
        self.cursor.execute("""SELECT id FROM hr_employee WHERE apps_id = %s AND slotted = 'f' limit 1""", (apps_id,))
        emp_id = self.cursor.fetchone()
        return emp_id.get('id') if emp_id else None
    
    
    def start(self):
        """This method is entry point for running the script
        """
        choice_list = [
                'not_slotted_with_DFR_wout_SERV',# 1
                'not_slotted_wout_SERVICE_DFR',  # 2
                'not_slotted_with_SERVICE_DFR',  # 3         
                'Exit'                           # 4
            ]
        while 1:
            self.result = []
            print('\n\n')
            for index, choice in enumerate(choice_list):
                print(str((index+1))+': '+choice)
            user_input = False
            while not user_input:
                user_input = input('\nChoose from the above list: ')
                try:
                    user_input = int(user_input)
                    if not user_input in range(1, 5):
                        user_input = False
                except Exception as e:
                    user_input = False
                    print('\nPlease Enter a valid number!')
            if user_input == 4:
                print("==== Bye ====")
                exit()
            sheet_name = choice_list[user_input-1]
            table_name = False
            while not table_name:
                table_name = input('\nEnter table name: ')
            if user_input == 1:
                self.__process_not_slotted_with_dfr_without_service(table_name, sheet_name)
            elif user_input == 2:
                self.__process_not_slotted_without_service_dfr(table_name, sheet_name)
            elif user_input == 3:
                self.__process_not_slotted_with_service_dfr(table_name, sheet_name)


# Entry Point
Solve_DFR_Service_History().start()
