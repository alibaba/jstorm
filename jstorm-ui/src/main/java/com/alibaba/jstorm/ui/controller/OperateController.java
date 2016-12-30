package com.alibaba.jstorm.ui.controller;

import com.alibaba.jstorm.ui.utils.Command;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.io.IOException;
import java.util.Date;

/**
 * OperateController Created with jstorm-ui.
 *
 * @author penuel (penuel.leo@gmail.com)
 * @date 16/3/2 下午5:31
 * @desc
 */
@Controller
@RequestMapping( "operate" )
public class OperateController {

    private Logger LOGGER = LoggerFactory.getLogger(OperateController.class);

    @RequestMapping( method = RequestMethod.POST, value = "jar", produces = "application/json;charset=utf-8" )
    @ResponseBody
    public String jar(HttpServletRequest request, @RequestParam( "file" ) CommonsMultipartFile file, @RequestParam( "classPath" ) String classPath) {
        JSONObject object = new JSONObject();
        try {
            String savePath = saveFile(request, file);
            String result = Command.Processor.execute(Command.Builder.jar(savePath, classPath));
            object.put("code", 0);
            object.put("msg", result);
        } catch ( Exception e ) {
            LOGGER.error("restart error for topology class [" + classPath + "]", e);
            object.put("code", 1);
            object.put("msg", "start jar fail:\n" + e.getMessage());
        }
        return object.toString();
    }

    @RequestMapping( method = RequestMethod.POST, value = "restart", produces = "application/json;charset=utf-8" )
    @ResponseBody
    public String restart(HttpServletRequest request, @RequestParam( value = "file", required = false ) CommonsMultipartFile file,
            @RequestParam( "topologyName" ) String topologyName) {
        JSONObject object = new JSONObject();
        try {
            String savePath = saveFile(request, file);
            String result = Command.Processor.execute(Command.Builder.restart(topologyName, savePath));
            object.put("code", 0);
            object.put("msg", result);
        } catch ( Exception e ) {
            LOGGER.error("restart error for topology [" + topologyName + "]", e);
            object.put("code", 1);
            object.put("msg", "restart fail:\n" + e.getMessage());
        }
        return object.toString();
    }

    @RequestMapping( method = RequestMethod.POST, value = "more", produces = "application/json;charset=utf-8" )
    @ResponseBody
    public String more(@RequestParam( "topologyName" ) String topologyName, @RequestParam( "action" ) int action) {
        JSONObject object = new JSONObject();
        CommandLine commandLine;
        try {
            switch ( action ) {
            case 0:
                commandLine = Command.Builder.activate(topologyName);
                break;
            case 1:
                commandLine = Command.Builder.deactivate(topologyName);
                break;
            case 2:
                commandLine = Command.Builder.kill(topologyName, 0);
                break;
            case 3:
                commandLine = Command.Builder.rebalance(topologyName, 0);
                break;
            default:
                object.put("code", 1);
                object.put("msg", "unrecognized command " + action + " for toplogy [" + topologyName + "]");
                return object.toString();
            }
            String result = Command.Processor.execute(commandLine);
            object.put("code", 0);
            object.put("msg", StringUtils.isBlank(result) ? "success" : result);

        } catch ( Exception e ) {
            LOGGER.error("more error action " + action + ",command for topoloty [" + topologyName + "]", e);
            object.put("code", 1);
            object.put("msg", "execute command fail! \n" + e.getMessage());
        }
        return object.toString();
    }

    private String saveFile(HttpServletRequest request, CommonsMultipartFile file) throws IOException {
        if ( null == file ){
            return "";
        }
        String savePath = "upload_files" + File.separator + DateFormatUtils.format(new Date(), "yyyyMMdd") + File.separator + file.getOriginalFilename();
        savePath = request.getSession().getServletContext().getRealPath(savePath);
        File saveFile = new File(savePath);
        if ( !saveFile.getParentFile().exists() ) {
            saveFile.getParentFile().mkdirs();
        }
        file.transferTo(saveFile);
        return saveFile.getAbsolutePath();
    }

}
