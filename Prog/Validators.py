def validate_command(keys):
    """
    :param keys: Message to show to the user
    Retrieve a valid positive integer from the the user keyboard.
    """
    good_number = False
    num = None

    while not good_number:
        try:
            num = int(raw_input("Input a number in {}: ".format(keys)))

            if num not in keys:
                raise ValueError("Please input a valid listed Key")
            elif num < 0:
                raise ValueError("Please input only a positive integer")

            good_number = True
        except ValueError as e:
            print e

    return num


def validate_num(message):
    """
    Guarantee that the user enters a non-negative number

    :param message: Prompt to display to user
    :return:
    """
    good_number = False
    num = None

    while not good_number:
        try:
            num = int(raw_input(message))

            if num < 0:
                raise ValueError("Please input only a non-negative integer")

            good_number = True
        except ValueError:
            print "Please input only a positive integer"

    return num
