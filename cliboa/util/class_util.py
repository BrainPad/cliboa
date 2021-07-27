from cliboa.conf import env


class ClassUtil(object):

    def is_custom_cls(self, cls_name):
        """
        Check if given class is a custom class.

        Args:
            cls_name: Class name

        Return:
            True: Custom class
            False: Default class
        """
        custom_classes = env.COMMON_CUSTOM_CLASSES + env.PROJECT_CUSTOM_CLASSES
        for cc in custom_classes:
            split_cc = cc.split(".")
            custom_cls_name = split_cc[1]
            if cls_name == custom_cls_name:
                return True
        return False

    def describe_class(self, cls_name):
        """
        Returns a pair of module path and class name by given name.

        Args:
            cls_name: Class name

        Return (tuple):
            tuple:
                - module path
                - class name
        """
        custom_cls_candidates = env.COMMON_CUSTOM_CLASSES + env.PROJECT_CUSTOM_CLASSES
        module = None
        for c in custom_cls_candidates:
            s = c.split(".")
            if s[-1:][0] == cls_name:
                module = s
                break

        if module is None:
            return None

        root = ".".join(module[:-1])
        mod_name = module[-1:][0]

        return root, mod_name
