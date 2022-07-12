from utils.api_utils import MyModules
MyModules.load_all_modules()

if __name__ == "__main__":
    for name,module in MyModules.current_module_list.items():
        if name != 'ux':
            module.run_test()