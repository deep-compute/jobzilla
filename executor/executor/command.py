from basescript import BaseScript

from .executor import Executor

class ExecutorCommand(BaseScript):
    DESC = 'Request and execute jobs'

    VERSION = 'api/v1'
    PROTOCOL = 'json'

    def run(self):
        e = Executor(
                url=self.args.url,
                version=self.VERSION,
                protocol=self.PROTOCOL,
                hostname=self.hostname,
                external_inp_dir=self.args.external_inp_dir,
                external_out_dir=self.args.external_out_dir,
                external_work_dir=self.args.external_work_dir,
                log=self.log,
            )

        e.run()

    def define_args(self, parser):
        parser.add_argument('url', help='master url')
        parser.add_argument('--external_inp_dir',
                            default='/executor/input/',
                            help='input mount point of central storage machine')
        parser.add_argument('--external_out_dir',
                            default='/executor/output/',
                            help='output mount point of central storage machine')
        parser.add_argument('--external_work_dir',
                            default='/executor/work_dir/',
                            help='working mount point of central storage machine')

def main():
    ExecutorCommand().start()

if __name__ == '__main__':
    main()


