from utilities.exceptions import ConfigurationInvalidError
from service.template import TemplateRenderer
from email.message import EmailMessage
from mimetypes import guess_type
from smtplib import SMTP, SMTP_SSL, SMTPException


class DispatcherException(Exception):
    def __init__(self, message: str, smtp_exception: SMTPException = None):
        self.message = message
        self.smtp_exception = smtp_exception


class Dispatcher:
    _template_renderer: TemplateRenderer

    def __init__(self, host: str, port: int, user: str, password: str, encryption: str, templates_path: str = None):
        if 'ssl' == encryption:
            self._smtp = SMTP_SSL(host, port)
        elif 'starttls' == encryption or encryption is None:
            self._smtp = SMTP(host, port)
        else:
            raise ConfigurationInvalidError('Invalid encryption type "{}" for smtp configuration'.format(encryption))

        try:
            self._smtp.login(user, password)
        except SMTPException as error:
            raise DispatcherException('Failed to connect to SMTP Server', error)

        self._template_renderer = TemplateRenderer() if templates_path is None else TemplateRenderer(templates_path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._smtp.quit()

    def send_email(
        self,
        from_email: str,
        to_email: str,
        subject: str,
        template_path_html: str,
        template_path_text: str,
        template_variables: dict,
        file_attachments: dict
    ):
        message = self._mail_message(subject, from_email, to_email)

        content_text = self._template_renderer.render_template(template_path_text, template_variables)
        content_html = self._template_renderer.render_template(template_path_html, template_variables)

        message.set_content(content_text)
        message.add_alternative(content_html, subtype='html')

        self._attach_files_to_mail_message(message, file_attachments)
        self._send_mail_message(message)

    def send_text_email(
        self,
        from_email: str,
        to_email: str,
        subject: str,
        template_path_text: str,
        template_variables: dict,
        file_attachments: dict = None
    ):
        message = self._mail_message(subject, from_email, to_email)
        content = self._template_renderer.render_template(template_path_text, template_variables)

        message.set_content(content)

        self._attach_files_to_mail_message(message, file_attachments)
        self._send_mail_message(message)

    def send_html_email(
        self,
        from_email: str,
        to_email: str,
        subject: str,
        template_path_html: str,
        template_variables: dict,
        file_attachments: dict = None
    ):
        message = self._mail_message(subject, from_email, to_email)
        content = self._template_renderer.render_template(template_path_html, template_variables)

        message.set_content(content, subtype='html')

        self._attach_files_to_mail_message(message, file_attachments)
        self._send_mail_message(message)

    def _send_mail_message(self, message: EmailMessage):
        try:
            self._smtp.send_message(message)
        except SMTPException as error:
            raise DispatcherException('Failed to send email', error)

    @staticmethod
    def _mail_message(subject: str, from_email: str, to_email: str) -> EmailMessage:
        message = EmailMessage()

        message['Subject'] = subject
        message['From'] = from_email
        message['To'] = to_email

        return message

    @staticmethod
    def _attach_files_to_mail_message(message: EmailMessage, file_attachments: dict):
        if type(file_attachments) is dict and 0 < len(file_attachments):
            for filename, filepath in file_attachments.items():
                with open(filepath, 'rb') as file:
                    file_data = file.read()
                    maintype, _, subtype = (guess_type(filepath)[0] or 'application/octet-stream').partition("/")
                    message.add_attachment(file_data, maintype=maintype, subtype=subtype, filename=filename)
