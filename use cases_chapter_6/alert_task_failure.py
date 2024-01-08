import smtplib
from email.message import EmailMessage
from metadata_plugin.utils import get_annotations

class AlertUtils:

    @staticmethod
    def send_email(subject, body, to_emails):
        """
        Send an email alert.
        """
        msg = EmailMessage()
        msg.set_content(body)
        msg["Subject"] = subject
        msg["From"] = "alert@yourdomain.com"
        msg["To"] = ", ".join(to_emails)

        with smtplib.SMTP("localhost", port=2525) as server:  
            server.send_message(msg)

    @staticmethod
    def handle_task_failure(context):
        """
        Handler called when a task fails.
        """
        task_instance = context['task_instance']
        metadata = get_annotations(task_instance.task)
        
        if not metadata:
            return

        # Check for high data_sensitivity_level
        if metadata.get("data_sensitivity_level") == "high":
            AlertUtils.send_email(
                subject=f"High-Priority Alert: Task {task_instance.task_id} failed",
                body=f"The task {task_instance.task_id} with high data sensitivity level has failed.",
                to_emails=["team_lead@yourdomain.com", "data_security@yourdomain.com"]
            )

