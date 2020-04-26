import mysql.connector as mysql
import mysql.connector.errors as mysql_errors
import yaml
from datetime import datetime
import gistat
import re
import os


class Cron:
    STATUS_FAILED = 0
    STATUS_SUCCESS = 1
    STATUS_WARNING = 2

    IN_PROGRESS = 1
    NO_PROGRESS = 0

    def __init__(self, debug=False):
        self.__load_config()
        self.__connect_to_db()
        self.start_time = None
        self.finished_time = None
        self.debug = debug

    def execute(self):
        self.start_time = self.date_now()
        self.__debug('Started execution')

        cron_details = self.__get_cron_details()

        self.__debug('Got cron details: {}'.format(cron_details))

        if cron_details is None:
            print('Cron is not active or not found')
            exit()

        if cron_details['in_progress'] == 1:
            error_msg = 'Cron already was started'

            self.__save_cron_history(self.STATUS_WARNING, error_msg)
            print(error_msg)
            exit()

        self.__start_progress()

        self.__debug('Starting parsing')

        try:
            # Getting the statistics
            with gistat.GiStat(debug=self.debug, firefox_path=self.config['cron']['firefox_path']) as stat:
                general_stat = stat.get_general_stat()
                updated = stat.get_update_time()

                self.__add_main_stat(general_stat, updated)
                other_cases = stat.get_other_cases()

                self.__add_by_gender_stat(other_cases['men'], other_cases['women'], updated)
                self.__add_pregnant_stat(other_cases['pregnant'], updated)
                self.__add_cases_by_type(other_cases['cases_local'], other_cases['cases_imported'], updated)

                cases_by_age = stat.get_cases_by_age()
                self.__add_cases_by_age(cases_by_age, updated)

                cases_by_city = stat.get_full_cases_by_city()
                self.__add_cases_by_city(cases_by_city, updated)

                self.__stop_progress()
                self.__debug('Finished parsing')
        except:
            self.__debug('Exception happen!')
            self.__save_cron_history(
                self.STATUS_FAILED,
                'Error Exception in Cron {}'.format(self.config['cron']['name'])
            )

            self.__stop_progress(False)

    @staticmethod
    def date_now():
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def __add_main_stat(self, data, updated):
        params = [
            data['confirmed'],
            data['recovered'],
            data['suspected'],
            data['deaths'],
            data['monitored'],
            updated,
            self.date_now()
        ]

        self.cursor.execute('INSERT INTO `main` (confirmed_cases,recovered_cases,suspected_cases,deaths,'
                            'monitored_cases,updated,added) VALUES (%s,%s,%s,%s,%s,%s,%s)', params)
        self.db.commit()

    def __add_by_gender_stat(self, male_cases, female_cases, updated):
        params = [
            male_cases,
            female_cases,
            updated,
            self.date_now()
        ]

        self.cursor.execute('INSERT INTO `cases_by_genders` (male_cases,female_cases,updated,added) VALUES (%s,%s,%s,'
                            '%s)', params)
        self.db.commit()

    def __add_pregnant_stat(self, cases, updated):
        params = [
            cases,
            updated,
            self.date_now()
        ]

        self.cursor.execute('INSERT INTO `cases_pregnant` (cases,updated,added) VALUES (%s,%s,%s)', params)
        self.db.commit()

    def __add_cases_by_type(self, local_cases, imported_cases, updated):
        params = [
            local_cases,
            imported_cases,
            updated,
            self.date_now()
        ]

        self.cursor.execute('INSERT INTO `cases_by_type` (local_cases,imported_cases,updated,added) VALUES (%s,%s,'
                            '%s,%s)', params)
        self.db.commit()

    def __add_cases_by_city(self, data, updated):
        values = []
        for row in data:
            self.cursor.execute('SELECT `id` FROM `cites` WHERE `name` = %s LIMIT 1', [row['city']])
            city = self.cursor.fetchone()

            if city is None:
                self.cursor.execute('INSERT INTO `cites` VALUES (NULL, %s)', [row['city']])
                self.db.commit()
                city_id = self.cursor.lastrowid
            else:
                city_id = city['id']

            values.append([
                city_id,
                row['confirmed'],
                row['recovered'],
                row['monitored'],
                row['deaths'],
                updated,
                self.date_now(),
            ])

        self.cursor.executemany('INSERT INTO `cases_by_city` (`city_id`,`confirmed_cases`,`recovered_cases`,'
                                '`monitored_cases`,`deaths`,`updated`,`added`) VALUES (%s,%s,%s,%s,%s,%s,%s)', values)
        self.db.commit()

    def __add_cases_by_age(self, data, updated):
        values = []
        for row in data:
            x = re.search("([0-9]+)?[<>-]([0-9]+)", row['range'])

            if x is None:
                # Ignore not standard range
                continue

            if row['type'] == 'ani':
                _type = 'years'
            else:
                _type = 'months'

            sql = 'SELECT `id` FROM `ages` WHERE`to` = %s AND `type` = %s'
            params = [
                x.group(2),
                _type
            ]

            if x.group(1) is None:
                sql += ' AND `from` IS NULL'
            else:
                sql += ' AND `from` = %s'
                params.append(x.group(1))

            sql += ' LIMIT 1'

            self.cursor.execute(sql, params)

            ages = self.cursor.fetchone()

            if ages is None:
                self.cursor.execute('INSERT INTO `ages` VALUES (NULL, %s, %s, %s)', [x.group(1), x.group(2), _type])
                self.db.commit()
                ages_id = self.cursor.lastrowid
            else:
                ages_id = ages['id']

            values.append([
                ages_id,
                row['cases'],
                updated,
                self.date_now(),
            ])

        self.cursor.executemany(
            'INSERT INTO `cases_by_age` (`age_id`,`cases`,`updated`,`added`) VALUES (%s,%s,%s,%s)',
            values
        )

        self.db.commit()

    def __start_progress(self):
        self.__set_progress(self.IN_PROGRESS)

    def __stop_progress(self, is_success=True):
        self.__set_progress(self.NO_PROGRESS, is_success)

    def __set_progress(self, progress_status, is_success=None):
        sql = 'UPDATE `cron_manager` SET `in_progress` = %s'
        params = [progress_status]

        # Set last Success if the execution was with success
        if progress_status == self.NO_PROGRESS and is_success:
            sql += ', `last_success` = %s'
            params.append(self.date_now())

        sql += ' WHERE `cron_name` = %s'
        params.append(self.config['cron']['name'])

        self.cursor.execute(sql, params)
        self.db.commit()

    def __save_cron_history(self, status, message=None):
        # If finished time not was set, we will get actual time
        if self.finished_time is None:
            finished_at = self.date_now()
        else:
            finished_at = self.finished_time

        self.cursor.execute('INSERT INTO `cron_history` (id,status,log_message,started_at,finished_at) VALUES (NULL,%s,'
                            '%s,%s,%s)', [status, message, self.start_time, finished_at])

        self.db.commit()

    def __load_config(self):
        with open("config/config.yaml", 'r') as stream:
            try:
                self.config = yaml.safe_load(stream)
                self.database_config = self.config['database']
            except yaml.YAMLError:
                raise CronException('Not found configuration')
            except KeyError as exc:
                raise CronException('Configuration not contain key: {}'.format(exc))

    def __connect_to_db(self):
        try:
            self.db = mysql.connect(
                host=self.database_config['host'],
                port=self.database_config['port'],
                user=self.database_config['user'],
                passwd=self.database_config['password'],
                database=self.database_config['database']
            )

            self.cursor = self.db.cursor(dictionary=True)
        except (mysql_errors.ProgrammingError, mysql_errors.InterfaceError) as exc:
            raise CronException('Cannot connect to database. Got error: {}'.format(exc))

    def __get_cron_details(self):
        self.cursor.execute(
            'SELECT * FROM `cron_manager` WHERE `cron_name` = %s AND `active` = 1',
            [self.config['cron']['name']]
        )

        return self.cursor.fetchone()

    def __debug(self, message):
        if self.debug:
            print('{} - {}'.format(self.date_now(), message))


class CronException(Exception):
    pass


if __name__ == '__main__':
    ROOT = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

    os.chdir(ROOT)

    gistat_cron = Cron(debug=True)
    gistat_cron.execute()
