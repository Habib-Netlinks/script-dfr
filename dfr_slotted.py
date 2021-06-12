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
            'database': '',
            'user': '',
            'password': '',
            'host': '',
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
        sheet.write(0, 1, 'msg', wb_format)
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
        self.cursor.execute("""SELECT date_joined_the_force AS enrol_date FROM hr_employee WHERE apps_id = %s 
                                AND active = 't' LIMIT 1""", (apps_id,))
        return self.cursor.fetchone()
    
    def get_dfr_date(self, emp_id):
        self.cursor.execute("""SELECT effective_date, id FROM dfr_status WHERE hr_employee_id = %s AND active = 't' AND state
                            in ('approved_by_hr', 'approved_by_grc', 'approved_by_prc_gdop') AND effective_date IS NOT NULL""", (emp_id,))
        return self.cursor.fetchall()
    
    
    def __process_slotted_without_service_dfr(self, table_name, sheet_name): #1
        """Process slotted records without service and dfr
        """
        records = self.__from_DB(table_name)
        counter = 0
        for rec in records:
            apps_id = rec.get('apps_id')
            if not apps_id:
                continue
            emp_id = self.__get_hr_employee_id(apps_id)
            enrol_date = self.get_enrollment_date(apps_id)
            if not emp_id:
                print('employee id not found for '+str(apps_id))
                self.result.append({'apps_id': str(apps_id), 'msg': 'Employee is not slotted / پرسونل تعیین بست نیست'})
                continue
            if enrol_date and enrol_date.get('enrol_date'):
                enrol_date = enrol_date.get('enrol_date')
                try:
                    self.cursor.execute("BEGIN")
                    self.cursor.execute("""SELECT id FROM employee_service_history WHERE hr_employee_id = %s AND active='t'""", (emp_id,))
                    res = self.cursor.fetchone()
                    if res and res.get('id'):
                        self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Service history already exist / تاریخ خدمت ازقبل موجود است'})
                        self.db_connection.rollback()
                        continue
                    else:
                        self.cursor.execute("""INSERT INTO employee_service_history (hr_employee_id, start_date, active, create_date)
                                        VALUES (%s, %s, 't', %s)""", (emp_id, str(enrol_date), datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') ))
                        self.db_connection.commit()
                        # self.db_connection.rollback()
                        counter += 1
                        print(table_name+': '+str(counter) + ' Done!')
                        self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Issue Fixed / مشکل حل گردید'})
                except Exception as e:
                    self.db_connection.rollback()
                    self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': str(e)})
                    print(e)
            else:
                self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Enrollment date is empty / تاریخ نشات ندارد'})
                print('Enrollment date not found for '+str(apps_id))
        print('\n\n----------- Total updated records '+str(counter))
        self.__create_excel_report(sheet_name, counter)
    
    def __process_slotted_with_service_dfr(self, table_name, sheet_name): #2
        """Process slotted records with service and dfr
        """
        records = self.__from_DB(table_name)
        counter = 0
        for rec in records:
            apps_id = rec.get('apps_id')
            if not apps_id:
                continue
            emp_id = self.__get_hr_employee_id(apps_id)
            if not emp_id:
                print('employee id not found for '+str(apps_id))
                self.result.append({'apps_id': str(apps_id), 'msg': 'Employee is not slotted / پرسونل تعیین بست نیست'})
                continue
            dfr_date = self.get_dfr_date(emp_id)
            if not dfr_date:
                self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'there is no approved dfr record for employee / هیچ زیکارد فعال یافت نشد dfr'})
                print('there is no approved dfr record for: '+str(apps_id))
                continue
            if len(dfr_date) > 1:
                self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Employee has more than 1 approved dfr / بیش از یک ریکارد فعال یافت شد dfr'})
                print('Employee has more than 1 approved dfr: '+str(apps_id))
                continue
            dfr_date = dfr_date[0].get('effective_date')
            try:
                self.cursor.execute("BEGIN")
                self.cursor.execute("""SELECT end_date, id FROM employee_service_history 
                                    WHERE hr_employee_id = %s 
                                    AND active='t' ORDER BY end_date DESC LIMIT 1""", (emp_id,))
                service_end_date = self.cursor.fetchone()
                if not service_end_date or not service_end_date.get('id'):
                    self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Service history is empty / تاریخ خدمت ندارد'})
                    self.db_connection.rollback()
                    continue
                self.cursor.execute("""SELECT to_date, id FROM assignment_assignment WHERE hr_employee_id = %s
                                    AND to_date IS NOT NULL AND active='t' AND from_date < %s ORDER BY from_date DESC LIMIT 1""", (emp_id,str(dfr_date)))
                last_assign_end_date = self.cursor.fetchone()
                if not last_assign_end_date or not last_assign_end_date.get('id'):
                    self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'There is no assignment before dfr effective date or assignment end date is empty / تاریخ ختم تعیین بست خالی است یا هیچ ریکاردی قبل از نیست قبل از dfr'})
                    self.db_connection.rollback()
                    continue
                if service_end_date.get('end_date') == last_assign_end_date.get('to_date') == dfr_date:
                    self.cursor.execute("""SELECT from_date FROM assignment_assignment WHERE hr_employee_id = %s
                                        AND from_date > %s AND active='t' AND state != 'cancelled' ORDER BY from_date LIMIT 1""", 
                                        (emp_id, str(dfr_date)))
                    assign_effective_date = self.cursor.fetchone()
                    if assign_effective_date:
                        self.cursor.execute("""INSERT INTO employee_service_history (hr_employee_id, start_date, active, create_date)
                                        VALUES (%s, %s, 't', %s)""", (emp_id, str(assign_effective_date.get('from_date')), datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') ))
                        counter +=1
                        self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Issue Fixed / مشکل حل گردید'})
                    else:
                        self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'There is no assignment after dfr effective date / ریکارد تعیین بست موجود نیست بعد از تاریخ dfr'})
                        self.db_connection.rollback()
                        continue
                        
                else:
                    self.cursor.execute("""UPDATE employee_service_history SET end_date = %s WHERE id = %s""", 
                                        (str(dfr_date), service_end_date.get('id')))
                    self.cursor.execute("""UPDATE assignment_assignment SET to_date = %s WHERE id = %s""",
                                        (str(dfr_date), last_assign_end_date.get('id')))
                    self.cursor.execute("""SELECT from_date FROM assignment_assignment WHERE hr_employee_id = %s
                                        AND from_date > %s AND active='t' AND state != 'cancelled' ORDER BY from_date LIMIT 1""", 
                                        (emp_id, str(dfr_date)))
                    assign_effective_date = self.cursor.fetchone()
                    if assign_effective_date:
                        self.cursor.execute("""INSERT INTO employee_service_history (hr_employee_id, start_date, active, create_date)
                                        VALUES (%s, %s, 't', %s)""", (emp_id, str(assign_effective_date.get('from_date')), datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') ))
                        counter +=1
                        self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Issue Fixed / مشکل حل گردید'})
                    else:
                        self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'There is no assignment after dfr effective date / ریکارد تعیین بست موجود نیست بعد از تاریخ dfr'})
                        self.db_connection.rollback()
                        continue
                self.db_connection.commit()
                # self.db_connection.rollback()
                print(table_name+': '+str(counter) + ' Done!')
            except Exception as e:
                if 'overlap' in str(e):
                    self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Service history already exist / تاریخ خدمت ازقبل موجود است'})
                else:
                    self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': str(e)})
                self.db_connection.rollback()
                print(e) 
        print('\n\n----------- Total updated records '+str(counter))         
        self.__create_excel_report(sheet_name, counter)


    def __process_slotted_with_dfr_without_service(self, table_name, sheet_name): #3
        """Process slotted records with dfr and without service
        """
        records = self.__from_DB(table_name)
        counter = 0
        for rec in records:
            apps_id = rec.get('apps_id')
            if not apps_id:
                continue
            emp_id = self.__get_hr_employee_id(apps_id)
            enrol_date = self.get_enrollment_date(apps_id)
            if not emp_id:
                print('employee id not found for '+str(apps_id))
                self.result.append({'apps_id': str(apps_id), 'msg': 'Employee is not slotted / پرسونل تعیین بست نیست'})
                continue
            dfr_date = self.get_dfr_date(emp_id)
            if not dfr_date:
                self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'there is no approved dfr record for employee / هیچ زیکارد فعال یافت نشد dfr'})
                print('there is no approved dfr record for: '+str(apps_id))
                continue
            if len(dfr_date) > 1:
                self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Employee has more than 1 approved dfr / بیش از یک ریکارد فعال یافت شد dfr'})
                print('Employee has more than 1 approved dfr: '+str(apps_id))
                continue
            dfr_id = dfr_date[0].get('id')
            dfr_date = dfr_date[0].get('effective_date')
            if enrol_date and enrol_date.get('enrol_date'):
                enrol_date = enrol_date.get('enrol_date')
            else:
                self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Enrollment date is empty / تاریخ نشات ندارد'})
                print('Enrollment date not found for '+str(apps_id))
                continue
            try:
                self.cursor.execute("BEGIN")
                self.cursor.execute("""SELECT from_date FROM assignment_assignment WHERE hr_employee_id = %s
                                    AND from_date > %s AND active='t' AND state != 'cancelled' ORDER BY from_date limit 1""", (emp_id, str(dfr_date)))
                first_assign_effective_date = self.cursor.fetchone()
                if not first_assign_effective_date or not first_assign_effective_date.get('from_date'):
                    self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'There is no assignment after dfr effective date'})
                    self.db_connection.rollback()
                    continue
                self.cursor.execute("""SELECT end_date, id FROM employee_service_history WHERE hr_employee_id=%s AND active='t'""", (emp_id,))
                res = self.cursor.fetchone()
                if res and res.get('end_date'):
                    self.db_connection.rollback()
                    self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Service history already exist / تاریخ خدمت ازقبل موجود است'})
                    print('Service history already exist')
                    continue
                    # self.cursor.execute("""UPDATE employee_service_history SET end_date = %s WHERE id = %s""", (str(dfr_date), res.get('id')))
                self.cursor.execute("""SELECT to_date, id FROM assignment_assignment WHERE hr_employee_id = %s
                                    AND to_date IS NOT NULL AND from_date < %s AND active='t' 
                                    ORDER BY from_date DESC LIMIT 1""", (emp_id, str(dfr_date)))
                last_assign_end_date = self.cursor.fetchone()
                if not last_assign_end_date or not last_assign_end_date.get('to_date'):
                    self.db_connection.rollback()
                    self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'There is no assignment before dfr effective date or assignment end date is empty / تاریخ ختم تعیین بست خالی است یا هیچ ریکاردی قبل از نیست قبل از dfr'})
                    print('No assignment before DFR')
                    continue
                if last_assign_end_date != dfr_date:
                    self.cursor.execute("""UPDATE assignment_assignment SET to_date = %s WHERE id = %s """,
                                            ( str(dfr_date), last_assign_end_date.get('id')))
                    if dfr_date > last_assign_end_date.get('to_date'): # T & A
                        start = last_assign_end_date.get('to_date')
                        end = dfr_date
                    else:
                        start = dfr_date
                        end = last_assign_end_date.get('to_date')
                    self.cursor.execute("""DELETE FROM personnel_time_attendance WHERE hr_employee_id = %s
                                    AND date >= %s AND date <= %s""", (emp_id, start, end))
                self.cursor.execute("""INSERT INTO employee_service_history (hr_employee_id, start_date, end_date, dfr_id, dfr_date, active, create_date)
                                VALUES (%s, %s, %s, %s, %s, 't', %s)""", (emp_id, str(enrol_date), str(dfr_date), dfr_id, str(dfr_date), datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
                self.cursor.execute("""INSERT INTO employee_service_history (hr_employee_id, start_date, active, create_date)
                                    VALUES (%s, %s, 't', %s)""", (emp_id, str(first_assign_effective_date.get('from_date')), datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
                self.db_connection.commit()
                # self.db_connection.rollback()
                counter += 1
                self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Issue Fixed / مشکل حل گردید'})
                print(table_name+': '+str(counter) + ' Done!')
            except Exception as e:
                self.db_connection.rollback()
                if 'overlap' in str(e):
                    self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': 'Service history already exist / تاریخ خدمت ازقبل موجود است'})
                else:
                    self.result.append({'apps_id': str(apps_id), 'emp_id': emp_id, 'msg': str(e)})
                print(e)
        print('\n\n----------- Total updated records '+str(counter))
        self.__create_excel_report(sheet_name, counter)

    
    def __get_hr_employee_id(self, apps_id):
        """Get id from hr_employee for a specific record
        
        Args:
            apps_id (string): apps_id of the record
        
        Returns:
            integer: the id of record
            None: if record not found
        """
        self.cursor.execute("""SELECT id FROM hr_employee WHERE apps_id = %s AND slotted = 't' limit 1""", (apps_id,))
        emp_id = self.cursor.fetchone()
        return emp_id.get('id') if emp_id else None
    
    
    def start(self):
        """This method is entry point for running the script
        """
        choice_list = [
                'slotted_wout_SERVICE_DFR',      # 1
                'slotted_with_SERVICE_DFR',      # 2
                'slotted_with_DFR_wout_SERVICE', # 3       
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
                self.__process_slotted_without_service_dfr(table_name, sheet_name)
            elif user_input == 2:
                self.__process_slotted_with_service_dfr(table_name, sheet_name)
            elif user_input == 3:
                self.__process_slotted_with_dfr_without_service(table_name, sheet_name)


# Entry Point
Solve_DFR_Service_History().start()
