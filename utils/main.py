from utils.io_utils import MyModules
MyModules.load_all_modules()

if __name__ == "__main__":
    for module in MyModules.current_module_list:
        MyModules(module)run_test