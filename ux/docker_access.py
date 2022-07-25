import os
import docker
import datetime
import prettytable as pt

# SPECS/What we want to achieve as for controls
# stop prod
# start a tradeexecutor with flatten or unwind
# pyrun tradeexecutor flatten sysperp -> should stop tradeexecutor ungracefully first
# pyrun tradeexecutor unwind sysperp -> should stop tradeexecutor ungracefully first
# service --> docker ps

def docker_stop_prod():
    '''
        # Should stop the running instance of tradeexecutor
        # And start a tradeexecutor with flatten or unwind (ask user)
    '''
    # TODO
    pass

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
        return_msg = f'could not find running image {image}'
    return return_msg

def docker_status(image):
    '''
        Service state
        Function to stop all containers running from a given image
        @params: [image1, image2, etc...] a list of the images name to get the status for
    '''

    client = docker.from_env()
    cn_list = client.containers.list()

    # Format and set the table headers
    table = pt.PrettyTable(["IMAGE", "CREATED", "UP FROM"])
    table.align['IMAGE'] = 'l'
    table.align['CREATED'] = 'r'
    table.align['UP FROM'] = 'r'

    for img in image:
        # Filter out to keep only wanted image
        filtered_cn_list = [cn for cn in cn_list if len(cn.image.tags) > 0]
        filtered_cn_list = [cn for cn in filtered_cn_list if img in cn.image.tags[0]]
        rows_for_container_i = add_row_to_table(filtered_cn_list) # returns a list of rows
        for row_i in rows_for_container_i:
            table.add_row(row_i)

    return_msg = table

    if return_msg:
        return return_msg
    else:
        img_list_on_host = [img.tags[0] for img in client.images.list() if img.tags]
        img_list = [((img.split("/"))[1]).split(":")[0] for img in img_list_on_host if len(img.split("/")) > 1]
        if image not in img_list:
            return f'image {image} does not exist on host'
        else:
            return f'image {image} is stopped'

def docker_ps():
    '''
        @returns: container states as a prettytable (HTML compatible)
    '''
    client = docker.from_env()
    img_list_on_host = [img.tags[0] for img in client.images.list() if img.tags]
    img_list = [((img.split("/"))[1]).split(":")[0] for img in img_list_on_host if len(img.split("/")) > 1]
    container_states = docker_status(img_list)
    return container_states


def add_row_to_table(filtered_cn_list):
    '''
        @returns: a list of rows to add to the table
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
        # status += " ago " if prefix == "Created" else ""
        return status

    row_list = []
    for container in filtered_cn_list:
        container_id = container.id[:12]
        container_image = ((container.image.tags[0].split("/"))[1]).split(":")[0]
        container_createdAt = container.attrs['Created']
        container_startedAt = container.attrs['State']['StartedAt']   # --> deduct uptime
        container_createdAt = datetime.datetime.strptime(container_createdAt[:-7], '%Y-%m-%dT%H:%M:%S.%f').replace(tzinfo=datetime.timezone.utc)
        container_startedAt = datetime.datetime.strptime(container_startedAt[:-7], '%Y-%m-%dT%H:%M:%S.%f').replace(tzinfo=datetime.timezone.utc)
        createdAt_duration_in_s = int((datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc) - container_createdAt).total_seconds())
        startedAt_duration_in_s = int((datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc) - container_startedAt).total_seconds())

        container_createdAt = _format_time(createdAt_duration_in_s, "")
        container_runningFrom = _format_time(startedAt_duration_in_s, "")
        container_status = container.attrs['State']['Status']
        container_name = container.name

        # columns = ["IMAGE", "CREATED", "UP FROM"]
        row_i = [container_image, container_createdAt, container_runningFrom]
        row_list.append(row_i)
        # return_msg = "<pre>" + tabulate(row_list, columns, tablefmt="grid") + "</pre>"

    return row_list

    # for img in img_list:
    #     filtered_cn_list = [cn for cn in img_list if img in (((cn.img.tags[0]).split("/"))[1]).split(":")[0] for img in img_list]

    # print(filtered_cn_list)

def main(*args):
    args = list(*args)
    # docker_state = docker_status(args)
    container_states = docker_ps
    print(container_states)