import os
import docker
import datetime
from tabulate import tabulate

# SPECS/What we want to achieve as for controls
# stop prod
# start a tradeexecutor with flatten or unwind
# pyrun tradeexecutor flatten sysperp -> should stop tradeexecutor ungracefully first
# pyrun tradeexecutor unwind sysperp -> should stop tradeexecutor ungracefully first

def docker_stop(image):
    '''
        Function to stop all containers running from a given image
        @params: image str of the image name to stop
    '''
    client = docker.from_env()
    cn_list = client.containers.list()
    filtered_cn_list = [cn for cn in cn_list if image in cn.image.tags[0]]
    return_msg = ""
    if len(filtered_cn_list) != 0:
        for container in filtered_cn_list:
            container_name = container.name
            container.stop(timeout=0)
            return_msg += f'image {image} --> stopped container {container_name}' + os.linesep
    else:
        return_msg = f'image {image} --> all containers already stopped'
    return return_msg

def docker_status(image):
    '''
        Service state
        Function to stop all containers running from a given image
        @params: image str of the image name to stop
    '''

    def _format_time(duration_in_s, prefix):
        days = divmod(duration_in_s, 86400)  # Get days (without [0]!)
        hours = divmod(days[1], 3600)  # Use remainder of days to calc hours
        minutes = divmod(hours[1], 60)  # Use remainder of hours to calc minutes
        seconds = divmod(minutes[1], 1)  # Use remainder of minutes to calc seconds
        status = prefix + " " + str(days[0]) + "d" if days[0] != 0 else ""
        status += str(hours[0]) + "h" if hours[0] != 0 else ""
        status += str(minutes[0]) + "mn" if minutes[0] != 0 else ""
        status += str(seconds[0]) + "s"
        status += " ago " if prefix == "Created" else ""
        return status

    client = docker.from_env()
    cn_list = client.containers.list()
    filtered_cn_list = [cn for cn in cn_list if image in cn.image.tags[0]]
    return_msg = ""
    row_list = []

    for container in filtered_cn_list:
        container_id = container.id[:12]
        container_image = ((container.image.tags[0].split("/"))[1]).split(":")[0]
        container_createdAt = container.attrs['Created']
        container_startedAt = container.attrs['State']['StartedAt']   # --> deduct uptime
        container_createdAt = datetime.datetime.strptime(container_createdAt[:-7], '%Y-%m-%dT%H:%M:%S.%f')
        container_startedAt = datetime.datetime.strptime(container_startedAt[:-7], '%Y-%m-%dT%H:%M:%S.%f')
        createdAt_duration_in_s = int((datetime.datetime.now() - container_createdAt).total_seconds())
        startedAt_duration_in_s = int((datetime.datetime.now() - container_startedAt).total_seconds())

        container_createdAt = _format_time(createdAt_duration_in_s, "Created")
        container_runningFrom = _format_time(startedAt_duration_in_s, "Status : Up")
        container_status = container.attrs['State']['Status']
        container_name = container.name

        columns = ["CONTAINER ID", "IMAGE", "CREATED", "UP FROM", "STATUS", "NAMES"]
        row_i = [container_id, container_image, container_createdAt, container_runningFrom, container_status, container_name]
        row_list.append(row_i)
        return_msg = tabulate(row_list, columns, tablefmt="grid")

    if return_msg:
        return return_msg
    else:
        img_list_on_host = [img.tags[0] for img in client.images.list() if img.tags]
        img_list = [((img.split("/"))[1]).split(":")[0] for img in img_list_on_host if len(img.split("/"))>1]
        if image not in img_list:
            return f'image {image} does not exist on host'
        else:
            return f'image {image} is stopped'
