# Generated by Django 2.0.7 on 2018-09-26 02:03

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('django_hpc_job_controller', '0002_websockettoken_is_file'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='hpcjob',
            name='cpus',
        ),
        migrations.RemoveField(
            model_name='hpcjob',
            name='ram',
        ),
        migrations.AddField(
            model_name='hpcjob',
            name='job_details',
            field=models.TextField(default=''),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='hpcjob',
            name='job_parameters',
            field=models.BinaryField(default=b''),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='hpcjob',
            name='job_pending_time',
            field=models.DateTimeField(blank=True, default=None, null=True),
        ),
        migrations.AddField(
            model_name='hpcjob',
            name='remote_job_id',
            field=models.BigIntegerField(default=0),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name='hpcjob',
            name='job_status',
            field=models.IntegerField(default=0, null=True),
        ),
        migrations.AlterField(
            model_name='hpcjob',
            name='job_submitted_time',
            field=models.DateTimeField(blank=True, default=None, null=True),
        ),
    ]
